//! Domain module for activity analysis transformations.
//!
//! This module handles transformation of activity data including:
//! - Stream data downsampling and statistics
//! - Interval summarization
//! - Best efforts compaction
//! - Power/HR/pace curve analysis
//! - Histogram transformations
//!
//! # GRASP Principles
//! - **Information Expert**: Activity analysis logic is encapsulated here
//! - **Low Coupling**: Minimal dependencies on other modules
//! - **High Cohesion**: Focused on activity data transformations only

use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::types::StreamWindow;

const KEY_DURATIONS: &[u32] = &[1, 5, 15, 30, 60, 120, 300, 600, 1200, 1800, 3600];

pub fn transform_streams(
    value: Value,
    max_points: Option<u32>,
    summary_only: bool,
    filter_streams: Option<Vec<String>>,
) -> Value {
    match value {
        Value::Array(streams) if is_stream_list(&streams) => {
            return transform_stream_list(
                streams,
                max_points,
                summary_only,
                filter_streams.as_deref(),
            );
        }
        Value::Object(mut obj) => {
            if let Some(streams) = obj.remove("streams") {
                let transformed =
                    transform_streams(streams, max_points, summary_only, filter_streams);
                obj.insert("streams".to_string(), transformed);
                return Value::Object(obj);
            }

            return transform_stream_object(
                obj,
                max_points,
                summary_only,
                filter_streams.as_deref(),
            );
        }
        other => return other,
    }
}

pub fn transform_streams_with_window(
    value: Value,
    max_points: Option<u32>,
    summary_only: bool,
    filter_streams: Option<Vec<String>>,
    window: Option<&StreamWindow>,
) -> Result<Value, String> {
    let Some(window) = window else {
        return Ok(transform_streams(
            value,
            max_points,
            summary_only,
            filter_streams,
        ));
    };

    let elapsed_window = ElapsedWindow::from_params(window)?;
    transform_windowed_streams(
        value,
        max_points,
        summary_only,
        filter_streams.as_deref(),
        &elapsed_window,
    )
}

fn transform_windowed_streams(
    value: Value,
    max_points: Option<u32>,
    summary_only: bool,
    filter_streams: Option<&[String]>,
    window: &ElapsedWindow,
) -> Result<Value, String> {
    match value {
        Value::Array(streams) if is_stream_list(&streams) => transform_stream_list_with_window(
            streams,
            max_points,
            summary_only,
            filter_streams,
            window,
        ),
        Value::Object(mut obj) => {
            if let Some(streams) = obj.remove("streams") {
                let transformed = transform_windowed_streams(
                    streams,
                    max_points,
                    summary_only,
                    filter_streams,
                    window,
                )?;
                obj.insert("streams".to_string(), transformed);
                return Ok(Value::Object(obj));
            }

            transform_stream_object_with_window(
                obj,
                max_points,
                summary_only,
                filter_streams,
                window,
            )
        }
        Value::Array(_) => {
            Err("window requires Intervals stream data with a time stream".to_string())
        }
        _ => Err("window requires stream data with a time stream".to_string()),
    }
}

fn transform_stream_object(
    obj: Map<String, Value>,
    max_points: Option<u32>,
    summary_only: bool,
    filter_streams: Option<&[String]>,
) -> Value {
    let mut result = Map::new();

    for (key, val) in obj {
        if !stream_matches_filter(&key, filter_streams) {
            continue;
        }

        let Some(arr) = val.as_array() else {
            result.insert(key.clone(), val.clone());
            continue;
        };

        if summary_only {
            result.insert(key.clone(), compute_stream_stats(arr));
        } else if let Some(max) = max_points {
            result.insert(
                key.clone(),
                Value::Array(downsample_array(arr, max as usize)),
            );
        } else {
            result.insert(key.clone(), val.clone());
        }
    }

    Value::Object(result)
}

fn transform_stream_object_with_window(
    obj: Map<String, Value>,
    max_points: Option<u32>,
    summary_only: bool,
    filter_streams: Option<&[String]>,
    window: &ElapsedWindow,
) -> Result<Value, String> {
    let time_data = find_time_stream_in_object(&obj)
        .ok_or_else(|| "window requires a time stream, but none was returned".to_string())?;
    let indices = window.indices_for(time_data);
    let mut result = Map::new();
    result.insert("window".to_string(), window.metadata(&indices, time_data));

    for (key, val) in obj {
        if !stream_matches_filter(&key, filter_streams) {
            continue;
        }

        let Some(arr) = val.as_array() else {
            result.insert(key.clone(), val.clone());
            continue;
        };

        let sliced = slice_array(arr, indices.start_index, indices.end_index);
        let value = transform_stream_array(&sliced, max_points, summary_only);
        result.insert(key, value);
    }

    Ok(Value::Object(result))
}

fn transform_stream_list(
    streams: Vec<Value>,
    max_points: Option<u32>,
    summary_only: bool,
    filter_streams: Option<&[String]>,
) -> Value {
    let mut result = Map::new();

    for stream in streams {
        let Some(obj) = stream.as_object() else {
            continue;
        };
        let stream_type = obj
            .get("type")
            .or_else(|| obj.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("unknown");

        if !stream_matches_filter(stream_type, filter_streams) {
            continue;
        }

        let Some(data) = obj.get("data").and_then(Value::as_array) else {
            continue;
        };

        let value = if summary_only {
            compute_stream_stats(data)
        } else if let Some(max) = max_points {
            Value::Array(downsample_array(data, max as usize))
        } else {
            Value::Array(data.clone())
        };
        result.insert(stream_type.to_string(), value);
    }

    Value::Object(result)
}

fn transform_stream_list_with_window(
    streams: Vec<Value>,
    max_points: Option<u32>,
    summary_only: bool,
    filter_streams: Option<&[String]>,
    window: &ElapsedWindow,
) -> Result<Value, String> {
    let time_data = streams
        .iter()
        .filter_map(Value::as_object)
        .find_map(|obj| {
            let stream_type = stream_type_from_object(obj);
            if stream_type.eq_ignore_ascii_case("time") {
                obj.get("data").and_then(Value::as_array)
            } else {
                None
            }
        })
        .ok_or_else(|| "window requires a time stream, but none was returned".to_string())?;

    let indices = window.indices_for(time_data);
    let mut result = Map::new();
    result.insert("window".to_string(), window.metadata(&indices, time_data));

    for stream in streams {
        let Some(obj) = stream.as_object() else {
            continue;
        };
        let stream_type = stream_type_from_object(obj);

        if !stream_matches_filter(stream_type, filter_streams) {
            continue;
        }

        let Some(data) = obj.get("data").and_then(Value::as_array) else {
            continue;
        };

        let sliced = slice_array(data, indices.start_index, indices.end_index);
        let value = transform_stream_array(&sliced, max_points, summary_only);
        result.insert(stream_type.to_string(), value);
    }

    Ok(Value::Object(result))
}

fn is_stream_list(streams: &[Value]) -> bool {
    streams
        .first()
        .and_then(Value::as_object)
        .is_some_and(|obj| {
            obj.contains_key("data") && (obj.contains_key("type") || obj.contains_key("name"))
        })
}

fn stream_matches_filter(stream_type: &str, filter_streams: Option<&[String]>) -> bool {
    let canonical_stream_type = canonical_stream_name(stream_type);
    filter_streams
        .map(|filter| {
            filter
                .iter()
                .any(|f| canonical_stream_name(f).eq_ignore_ascii_case(canonical_stream_type))
        })
        .unwrap_or(true)
}

fn canonical_stream_name(stream: &str) -> &str {
    if stream.eq_ignore_ascii_case("power") {
        "watts"
    } else {
        stream
    }
}

fn stream_type_from_object(obj: &Map<String, Value>) -> &str {
    obj.get("type")
        .or_else(|| obj.get("name"))
        .and_then(Value::as_str)
        .unwrap_or("unknown")
}

fn find_time_stream_in_object(obj: &Map<String, Value>) -> Option<&Vec<Value>> {
    obj.get("time").and_then(Value::as_array).or_else(|| {
        obj.iter()
            .find(|(key, _)| key.eq_ignore_ascii_case("time"))
            .and_then(|(_, value)| value.as_array())
    })
}

fn transform_stream_array(arr: &[Value], max_points: Option<u32>, summary_only: bool) -> Value {
    if summary_only {
        compute_stream_stats(arr)
    } else if let Some(max) = max_points {
        Value::Array(downsample_array(arr, max as usize))
    } else {
        Value::Array(arr.to_vec())
    }
}

fn slice_array(arr: &[Value], start_index: usize, end_index: usize) -> Vec<Value> {
    let start = start_index.min(arr.len());
    let end = end_index.min(arr.len()).max(start);
    arr[start..end].to_vec()
}

#[derive(Debug)]
struct ElapsedWindow {
    requested_start: String,
    requested_end: String,
    start_seconds: f64,
    end_seconds: f64,
}

#[derive(Debug)]
struct WindowIndices {
    start_index: usize,
    end_index: usize,
}

impl ElapsedWindow {
    fn from_params(params: &StreamWindow) -> Result<Self, String> {
        let window_type = params.window_type.as_deref().unwrap_or("elapsed_time");
        if !window_type.eq_ignore_ascii_case("elapsed_time") {
            return Err(format!(
                "unsupported stream window type '{window_type}', expected 'elapsed_time'"
            ));
        }

        let start_seconds = parse_elapsed_seconds(&params.start)?;
        let end_seconds = parse_elapsed_seconds(&params.end)?;
        if end_seconds <= start_seconds {
            return Err("stream window end must be after start".to_string());
        }

        Ok(Self {
            requested_start: params.start.clone(),
            requested_end: params.end.clone(),
            start_seconds,
            end_seconds,
        })
    }

    fn indices_for(&self, time_data: &[Value]) -> WindowIndices {
        let start_index = first_index_at_or_after(time_data, self.start_seconds);
        let end_index = first_index_at_or_after(time_data, self.end_seconds);
        WindowIndices {
            start_index,
            end_index: end_index.max(start_index),
        }
    }

    fn metadata(&self, indices: &WindowIndices, time_data: &[Value]) -> Value {
        let first_available_seconds = first_available_seconds(time_data);
        let last_available_seconds = last_available_seconds(time_data);
        let actual_start_seconds = seconds_at_index(time_data, indices.start_index);
        let actual_end_seconds =
            seconds_at_index(time_data, indices.end_index).or(last_available_seconds);
        let clamped_start = first_available_seconds.is_some_and(|first| self.start_seconds < first)
            || (actual_start_seconds.is_none() && indices.start_index >= time_data.len());
        let clamped_end = last_available_seconds.is_some_and(|last| self.end_seconds > last);

        serde_json::json!({
            "type": "elapsed_time",
            "requested_start": self.requested_start,
            "requested_end": self.requested_end,
            "start_seconds": self.start_seconds,
            "end_seconds": self.end_seconds,
            "actual_start_seconds": actual_start_seconds,
            "actual_end_seconds": actual_end_seconds,
            "clamped": clamped_start || clamped_end,
            "clamped_start": clamped_start,
            "clamped_end": clamped_end,
            "start_index": indices.start_index,
            "end_index": indices.end_index,
            "end_exclusive": true,
            "points": indices.end_index.saturating_sub(indices.start_index)
        })
    }
}

fn parse_elapsed_seconds(input: &str) -> Result<f64, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("elapsed time cannot be empty".to_string());
    }

    if let Ok(seconds) = trimmed.parse::<f64>() {
        if seconds.is_finite() && seconds >= 0.0 {
            return Ok(seconds);
        }
        return Err(format!(
            "elapsed time '{input}' must be a non-negative number"
        ));
    }

    let parts: Vec<&str> = trimmed.split(':').collect();
    let seconds = match parts.as_slice() {
        [minutes, seconds] => {
            parse_time_part(minutes, input)? * 60.0 + parse_time_part(seconds, input)?
        }
        [hours, minutes, seconds] => {
            parse_time_part(hours, input)? * 3600.0
                + parse_time_part(minutes, input)? * 60.0
                + parse_time_part(seconds, input)?
        }
        _ => {
            return Err(format!(
                "elapsed time '{input}' must use seconds, MM:SS, or HH:MM:SS"
            ));
        }
    };

    Ok(seconds)
}

fn parse_time_part(part: &str, original: &str) -> Result<f64, String> {
    let value = part
        .parse::<f64>()
        .map_err(|_| format!("elapsed time '{original}' contains an invalid component '{part}'"))?;
    if value.is_finite() && value >= 0.0 {
        Ok(value)
    } else {
        Err(format!(
            "elapsed time '{original}' contains a negative or invalid component '{part}'"
        ))
    }
}

fn first_index_at_or_after(time_data: &[Value], target_seconds: f64) -> usize {
    time_data
        .iter()
        .position(|value| value_to_seconds(value).is_some_and(|seconds| seconds >= target_seconds))
        .unwrap_or(time_data.len())
}

fn value_to_seconds(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|s| parse_elapsed_seconds(s).ok()))
}

fn seconds_at_index(time_data: &[Value], index: usize) -> Option<f64> {
    time_data.get(index).and_then(value_to_seconds)
}

fn first_available_seconds(time_data: &[Value]) -> Option<f64> {
    time_data.iter().find_map(value_to_seconds)
}

fn last_available_seconds(time_data: &[Value]) -> Option<f64> {
    time_data.iter().rev().find_map(value_to_seconds)
}

pub fn compute_stream_stats(arr: &[Value]) -> Value {
    let nums: Vec<f64> = arr
        .iter()
        .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
        .collect();

    if nums.is_empty() {
        return serde_json::json!({ "count": 0 });
    }

    let count = nums.len();
    let sum: f64 = nums.iter().sum();
    let avg = sum / count as f64;
    let min = nums.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = nums.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

    let mut sorted = nums.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p10 = sorted[count / 10];
    let p50 = sorted[count / 2];
    let p90 = sorted[count * 9 / 10];

    serde_json::json!({
        "count": count,
        "min": min,
        "max": max,
        "avg": (avg * 100.0).round() / 100.0,
        "p10": p10,
        "p50": p50,
        "p90": p90
    })
}

pub fn downsample_array(arr: &[Value], target: usize) -> Vec<Value> {
    let len = arr.len();
    if len <= target || target < 2 {
        return arr.to_vec();
    }

    let mut result = Vec::with_capacity(target);
    result.push(arr[0].clone());

    let step = (len - 1) as f64 / (target - 1) as f64;
    for i in 1..(target - 1) {
        let idx = (i as f64 * step).round() as usize;
        result.push(arr[idx.min(len - 1)].clone());
    }

    result.push(arr[len - 1].clone());
    result
}

pub fn transform_intervals(
    value: &Value,
    summary_only: bool,
    max_intervals: usize,
    fields: Option<&[String]>,
) -> Value {
    let activity_id = value
        .get("id")
        .or_else(|| value.get("activity_id"))
        .and_then(Value::as_str);

    let Some((source, arr)) = select_interval_array(value) else {
        return value.clone();
    };

    if summary_only {
        let total = arr.len();
        let mut type_counts: HashMap<String, usize> = HashMap::new();
        let mut zone_counts: HashMap<String, usize> = HashMap::new();
        let mut total_elapsed_time: f64 = 0.0;
        let mut total_moving_time: f64 = 0.0;
        let mut work_count = 0usize;
        let mut recovery_count = 0usize;
        let mut total_work_time: f64 = 0.0;

        for item in arr {
            if let Some(obj) = item.as_object() {
                if let Some(t) = obj.get("type").and_then(|v| v.as_str()) {
                    *type_counts.entry(t.to_string()).or_insert(0) += 1;
                    if t.eq_ignore_ascii_case("work") {
                        work_count += 1;
                        total_work_time +=
                            number_field(obj, &["moving_time", "elapsed_time"]).unwrap_or_default();
                    } else if t.eq_ignore_ascii_case("recovery") || t.eq_ignore_ascii_case("rest") {
                        recovery_count += 1;
                    }
                }
                if let Some(zone) = obj.get("zone").and_then(Value::as_i64) {
                    *zone_counts.entry(zone.to_string()).or_insert(0) += 1;
                }
                if let Some(elapsed_time) = number_field(obj, &["elapsed_time", "duration"]) {
                    total_elapsed_time += elapsed_time;
                }
                if let Some(moving_time) = number_field(obj, &["moving_time"]) {
                    total_moving_time += moving_time;
                }
            }
        }

        let mut summary = serde_json::json!({
            "count": total,
            "source": source,
            "types": type_counts,
            "zones": zone_counts,
            "work_count": work_count,
            "recovery_count": recovery_count,
            "total_elapsed_time": total_elapsed_time,
            "total_moving_time": total_moving_time,
            "total_work_time": total_work_time,
            "avg_elapsed_time": if total > 0 { total_elapsed_time / total as f64 } else { 0.0 }
        });

        if let Some(activity_id) = activity_id
            && let Some(obj) = summary.as_object_mut()
        {
            obj.insert(
                "activity_id".to_string(),
                Value::String(activity_id.to_string()),
            );
        }

        return summary;
    }

    let limited: Vec<Value> = arr
        .iter()
        .take(max_intervals)
        .filter_map(|item| item.as_object())
        .map(|obj| compact_interval(obj, fields))
        .collect();

    let mut result = Map::new();
    if let Some(activity_id) = activity_id {
        result.insert(
            "activity_id".to_string(),
            Value::String(activity_id.to_string()),
        );
    }
    result.insert("source".to_string(), Value::String(source.to_string()));
    result.insert("count".to_string(), Value::from(arr.len()));
    result.insert("max_intervals".to_string(), Value::from(max_intervals));
    result.insert(
        "truncated".to_string(),
        Value::from(arr.len() > max_intervals),
    );
    result.insert("intervals".to_string(), Value::Array(limited));

    Value::Object(result)
}

fn select_interval_array(value: &Value) -> Option<(&'static str, &Vec<Value>)> {
    if let Some(arr) = value.as_array() {
        return Some(("intervals", arr));
    }

    let obj = value.as_object()?;
    for key in ["icu_intervals", "intervals", "icu_groups"] {
        if let Some(arr) = obj.get(key).and_then(Value::as_array) {
            return Some((key, arr));
        }
    }

    None
}

fn compact_interval(obj: &Map<String, Value>, extra_fields: Option<&[String]>) -> Value {
    let mut result = Map::new();

    copy_field(obj, &mut result, "id");
    copy_field(obj, &mut result, "label");
    copy_field(obj, &mut result, "name");
    copy_field(obj, &mut result, "type");
    copy_field(obj, &mut result, "zone");
    copy_field(obj, &mut result, "zone_min_watts");
    copy_field(obj, &mut result, "zone_max_watts");

    if let Some(start_seconds) = number_field(obj, &["start_time", "start_seconds"]) {
        result.insert("start_seconds".to_string(), Value::from(start_seconds));
    }
    if let Some(end_seconds) = number_field(obj, &["end_time", "end_seconds"]) {
        result.insert("end_seconds".to_string(), Value::from(end_seconds));
    }
    copy_field(obj, &mut result, "start_index");
    copy_field(obj, &mut result, "end_index");
    copy_field(obj, &mut result, "elapsed_time");
    copy_field(obj, &mut result, "moving_time");

    if let Some(window) = interval_window(obj) {
        result.insert("window".to_string(), window);
    }

    let stats = interval_stats(obj);
    if !stats.is_empty() {
        result.insert("stats".to_string(), Value::Object(stats));
    }

    if let Some(extra_fields) = extra_fields {
        for field in extra_fields {
            if result.contains_key(field) {
                continue;
            }
            if let Some(value) = obj.get(field) {
                result.insert(field.clone(), value.clone());
            }
        }
    }

    Value::Object(result)
}

fn interval_window(obj: &Map<String, Value>) -> Option<Value> {
    let start_seconds = number_field(obj, &["start_time", "start_seconds"])?;
    let end_seconds = number_field(obj, &["end_time", "end_seconds"])?;

    Some(serde_json::json!({
        "type": "elapsed_time",
        "start": format_elapsed_time(start_seconds),
        "end": format_elapsed_time(end_seconds)
    }))
}

fn interval_stats(obj: &Map<String, Value>) -> Map<String, Value> {
    let mut stats = Map::new();
    for field in [
        "average_watts",
        "weighted_average_watts",
        "average_heartrate",
        "max_heartrate",
        "average_cadence",
        "average_torque",
        "max_torque",
    ] {
        copy_field(obj, &mut stats, field);
    }
    stats
}

fn copy_field(source: &Map<String, Value>, target: &mut Map<String, Value>, field: &str) {
    if let Some(value) = source.get(field)
        && !value.is_null()
    {
        target.insert(field.to_string(), value.clone());
    }
}

fn number_field(obj: &Map<String, Value>, fields: &[&str]) -> Option<f64> {
    fields.iter().find_map(|field| {
        obj.get(*field).and_then(|value| {
            value
                .as_f64()
                .or_else(|| value.as_i64().map(|number| number as f64))
                .or_else(|| value.as_u64().map(|number| number as f64))
        })
    })
}

fn format_elapsed_time(seconds: f64) -> String {
    let total_seconds = seconds.round().max(0.0) as u64;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    if hours > 0 {
        format!("{hours}:{minutes:02}:{seconds:02}")
    } else {
        format!("{minutes}:{seconds:02}")
    }
}

pub fn compact_intervals(value: &Value, fields: Option<&[String]>) -> Value {
    let default_fields = [
        "type",
        "start",
        "end",
        "duration",
        "intensity",
        "activity_id",
    ];
    let fields_to_use: Vec<&str> = fields
        .map(|f| f.iter().map(|s| s.as_str()).collect())
        .unwrap_or_else(|| default_fields.to_vec());

    let Some(arr) = value.as_array() else {
        return value.clone();
    };

    let compacted: Vec<Value> = arr
        .iter()
        .map(|item| {
            let Some(obj) = item.as_object() else {
                return item.clone();
            };
            let mut result = Map::new();
            for field in &fields_to_use {
                if let Some(val) = obj.get(*field) {
                    result.insert(field.to_string(), val.clone());
                }
            }
            Value::Object(result)
        })
        .collect();

    Value::Array(compacted)
}

pub fn summarize_best_efforts(value: &Value, stream: &str) -> Value {
    let Some(arr) = value.as_array() else {
        return value.clone();
    };

    let efforts: Vec<Value> = arr
        .iter()
        .filter_map(|item| {
            let obj = item.as_object()?;
            let mut compact = Map::new();

            if let Some(v) = obj.get("value") {
                compact.insert("value".to_string(), v.clone());
            }
            if let Some(v) = obj.get("duration") {
                compact.insert("duration".to_string(), v.clone());
            }
            if let Some(v) = obj.get("start_index") {
                compact.insert("start_index".to_string(), v.clone());
            }

            Some(Value::Object(compact))
        })
        .collect();

    serde_json::json!({
        "stream": stream,
        "count": efforts.len(),
        "efforts": efforts
    })
}

pub fn transform_curves(value: &Value, summary_only: bool, durations: Option<&[u32]>) -> Value {
    if summary_only
        && let Some(compact) = compact_curve_payload(value, durations.unwrap_or(KEY_DURATIONS))
    {
        return compact;
    }

    if let Some(dur_filter) = durations
        && let Some(obj) = value.as_object()
    {
        if obj.get("list").and_then(Value::as_array).is_some() {
            return filter_curve_payload(value, dur_filter).unwrap_or_else(|| value.clone());
        }

        let mut result = Map::new();
        for (key, val) in obj {
            if let Some(arr) = val.as_array() {
                let filtered: Vec<&Value> = arr
                    .iter()
                    .filter(|item| {
                        item.get("secs")
                            .and_then(|s| s.as_u64())
                            .map(|s| dur_filter.contains(&(s as u32)))
                            .unwrap_or(false)
                    })
                    .collect();
                result.insert(
                    key.clone(),
                    Value::Array(filtered.into_iter().cloned().collect()),
                );
            } else {
                result.insert(key.clone(), val.clone());
            }
        }
        return Value::Object(result);
    }

    if summary_only {
        if let Some(obj) = value.as_object() {
            let mut result = Map::new();
            for (key, val) in obj {
                if let Some(arr) = val.as_array() {
                    let filtered: Vec<&Value> = arr
                        .iter()
                        .filter(|item| {
                            item.get("secs")
                                .and_then(|s| s.as_u64())
                                .map(|s| KEY_DURATIONS.contains(&(s as u32)))
                                .unwrap_or(false)
                        })
                        .collect();
                    result.insert(
                        key.clone(),
                        Value::Array(filtered.into_iter().cloned().collect()),
                    );
                } else {
                    result.insert(key.clone(), val.clone());
                }
            }
            return Value::Object(result);
        }
    }

    value.clone()
}

fn compact_curve_payload(value: &Value, durations: &[u32]) -> Option<Value> {
    let payload = if value.get("list").is_some() {
        value
    } else {
        value.get("value")?
    };
    let list = payload.get("list")?.as_array()?;

    let mut curves = Vec::new();
    for curve in list {
        let secs = curve.get("secs").and_then(Value::as_array)?;
        let watts = curve.get("watts").and_then(Value::as_array)?;
        let watts_per_kg = curve.get("watts_per_kg").and_then(Value::as_array);

        let mut key_powers = Map::new();
        for (idx, sec) in secs.iter().enumerate() {
            let Some(sec) = sec.as_u64().map(|s| s as u32) else {
                continue;
            };
            if !durations.contains(&sec) || idx >= watts.len() {
                continue;
            }

            let mut point = Map::new();
            point.insert("watts".to_string(), watts[idx].clone());
            if let Some(wkg) = watts_per_kg
                .and_then(|arr| arr.get(idx))
                .and_then(Value::as_f64)
            {
                point.insert("w_kg".to_string(), Value::from(round_to(wkg, 2)));
            } else {
                point.insert("w_kg".to_string(), Value::Null);
            }
            key_powers.insert(format!("{}s", sec), Value::Object(point));
        }

        let mut entry = Map::new();
        entry.insert(
            "label".to_string(),
            curve
                .get("label")
                .cloned()
                .unwrap_or_else(|| Value::String(String::new())),
        );
        entry.insert("key_powers".to_string(), Value::Object(key_powers));
        entry.insert(
            "weight".to_string(),
            curve.get("weight").cloned().unwrap_or(Value::Null),
        );

        if let Some(models) = curve.get("powerModels").and_then(Value::as_array) {
            let ftp_estimates = models
                .iter()
                .map(|model| {
                    serde_json::json!({
                        "model": model.get("type").cloned().unwrap_or(Value::Null),
                        "ftp": model.get("ftp").cloned().unwrap_or(Value::Null),
                        "w_prime": model.get("wPrime").cloned().unwrap_or(Value::Null),
                    })
                })
                .collect();
            entry.insert("ftp_estimates".to_string(), Value::Array(ftp_estimates));
        }

        if let Some(vo2max) = curve.get("vo2max_5m").and_then(Value::as_f64) {
            entry.insert("vo2max".to_string(), Value::from(round_to(vo2max, 1)));
        }

        curves.push(Value::Object(entry));
    }

    let activities_count = payload
        .get("activities")
        .and_then(Value::as_object)
        .map(|activities| activities.len())
        .unwrap_or(0);

    Some(serde_json::json!({
        "curves": curves,
        "activities_count": activities_count
    }))
}

fn filter_curve_payload(value: &Value, durations: &[u32]) -> Option<Value> {
    let payload = value.as_object()?;
    let list = payload.get("list")?.as_array()?;
    let mut filtered_payload = payload.clone();

    let filtered_list = list
        .iter()
        .map(|curve| {
            let Some(curve_obj) = curve.as_object() else {
                return curve.clone();
            };
            let Some(secs) = curve_obj.get("secs").and_then(Value::as_array) else {
                return curve.clone();
            };

            let keep_indexes: Vec<usize> = secs
                .iter()
                .enumerate()
                .filter_map(|(idx, sec)| {
                    sec.as_u64()
                        .filter(|sec| durations.contains(&(*sec as u32)))
                        .map(|_| idx)
                })
                .collect();

            let mut filtered_curve = curve_obj.clone();
            for key in ["secs", "watts", "watts_per_kg"] {
                if let Some(arr) = curve_obj.get(key).and_then(Value::as_array) {
                    let filtered_values = keep_indexes
                        .iter()
                        .filter_map(|idx| arr.get(*idx).cloned())
                        .collect();
                    filtered_curve.insert(key.to_string(), Value::Array(filtered_values));
                }
            }
            Value::Object(filtered_curve)
        })
        .collect();

    filtered_payload.insert("list".to_string(), Value::Array(filtered_list));
    Some(Value::Object(filtered_payload))
}

pub fn transform_histogram(value: &Value, summary_only: bool, max_bins: usize) -> Value {
    if summary_only && let Some(arr) = value.as_array() {
        let mut total_count: f64 = 0.0;
        let mut weighted_sum: f64 = 0.0;
        let mut min_val: Option<f64> = None;
        let mut max_val: Option<f64> = None;

        for item in arr {
            let count = item
                .get("count")
                .or_else(|| item.get("secs"))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let value = item
                .get("value")
                .and_then(|v| v.as_f64())
                .or_else(|| {
                    match (
                        item.get("min").and_then(Value::as_f64),
                        item.get("max").and_then(Value::as_f64),
                    ) {
                        (Some(min), Some(max)) => Some((min + max) / 2.0),
                        (Some(min), None) => Some(min),
                        (None, Some(max)) => Some(max),
                        _ => None,
                    }
                })
                .unwrap_or(0.0);

            if count > 0.0 {
                total_count += count;
                weighted_sum += value * count;
                let bucket_min = item.get("min").and_then(Value::as_f64).unwrap_or(value);
                let bucket_max = item.get("max").and_then(Value::as_f64).unwrap_or(value);
                min_val = Some(min_val.map_or(bucket_min, |m: f64| m.min(bucket_min)));
                max_val = Some(max_val.map_or(bucket_max, |m: f64| m.max(bucket_max)));
            }
        }

        return serde_json::json!({
            "total_samples": total_count as u64,
            "weighted_avg": if total_count > 0.0 { (weighted_sum / total_count * 100.0).round() / 100.0 } else { 0.0 },
            "min": min_val.unwrap_or(0.0),
            "max": max_val.unwrap_or(0.0),
            "bins_available": arr.len()
        });
    }

    if let Some(arr) = value.as_array()
        && arr.len() > max_bins
    {
        let step = arr.len() / max_bins;
        let sampled: Vec<Value> = arr
            .iter()
            .step_by(step.max(1))
            .take(max_bins)
            .cloned()
            .collect();
        return Value::Array(sampled);
    }

    value.clone()
}

fn round_to(value: f64, decimals: u32) -> f64 {
    let factor = 10_f64.powi(decimals as i32);
    (value * factor).round() / factor
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_stats_empty_returns_zero_count() {
        let out = compute_stream_stats(&[]);
        assert_eq!(out["count"], 0);
    }

    #[test]
    fn downsample_preserves_edges() {
        let arr = vec![1, 2, 3, 4, 5]
            .into_iter()
            .map(Value::from)
            .collect::<Vec<_>>();
        let out = downsample_array(&arr, 3);
        assert_eq!(out.first(), Some(&Value::from(1)));
        assert_eq!(out.last(), Some(&Value::from(5)));
    }

    #[test]
    fn summarize_best_efforts_keeps_compact_shape() {
        let input = serde_json::json!([
            {"value": 300, "duration": 60, "start_index": 100, "ignored": true}
        ]);
        let out = summarize_best_efforts(&input, "power");
        assert_eq!(out["stream"], "power");
        assert_eq!(out["count"], 1);
        assert!(out["efforts"][0].get("ignored").is_none());
    }

    #[test]
    fn compute_stream_stats_single_value() {
        let arr = vec![serde_json::json!(42.5)];
        let stats = compute_stream_stats(&arr);
        assert_eq!(stats["count"], 1);
        assert_eq!(stats["min"], 42.5);
        assert_eq!(stats["max"], 42.5);
        assert_eq!(stats["avg"], 42.5);
        assert_eq!(stats["p10"], 42.5);
        assert_eq!(stats["p50"], 42.5);
        assert_eq!(stats["p90"], 42.5);
    }

    #[test]
    fn compute_stream_stats_multiple_values() {
        let arr = vec![
            serde_json::json!(10.0),
            serde_json::json!(20.0),
            serde_json::json!(30.0),
            serde_json::json!(40.0),
            serde_json::json!(50.0),
        ];
        let stats = compute_stream_stats(&arr);
        assert_eq!(stats["count"], 5);
        assert_eq!(stats["min"], 10.0);
        assert_eq!(stats["max"], 50.0);
        assert_eq!(stats["avg"], 30.0);
        assert_eq!(stats["p10"], 10.0);
        assert_eq!(stats["p50"], 30.0);
        assert_eq!(stats["p90"], 50.0);
    }

    #[test]
    fn compute_stream_stats_with_integers() {
        let arr = vec![
            serde_json::json!(1),
            serde_json::json!(2),
            serde_json::json!(3),
        ];
        let stats = compute_stream_stats(&arr);
        assert_eq!(stats["count"], 3);
        assert_eq!(stats["min"], 1.0);
        assert_eq!(stats["max"], 3.0);
        assert_eq!(stats["avg"], 2.0);
    }

    #[test]
    fn transform_streams_compacts_intervals_stream_array_shape() {
        let input = serde_json::json!([
            {"type": "time", "data": [0, 1, 2, 3]},
            {"type": "watts", "data": [100, 150, 200, null]},
            {"type": "cadence", "data": [80, 90, 100]}
        ]);

        let result = transform_streams(input, None, true, Some(vec!["watts".into()]));
        assert!(result.get("time").is_none());
        assert!(result.get("cadence").is_none());
        assert_eq!(result["watts"]["count"], 3);
        assert_eq!(result["watts"]["avg"], 150.0);
        assert_eq!(result["watts"]["p50"], 150.0);
    }

    #[test]
    fn transform_streams_downsamples_intervals_stream_array_shape() {
        let input = serde_json::json!([
            {"type": "watts", "data": [1, 2, 3, 4, 5]}
        ]);

        let result = transform_streams(input, Some(3), false, None);
        assert_eq!(result["watts"], serde_json::json!([1, 3, 5]));
    }

    #[test]
    fn transform_streams_window_slices_before_summary() {
        let input = serde_json::json!([
            {"type": "time", "data": [0, 60, 120, 180, 240]},
            {"type": "watts", "data": [100, 200, 300, 400, 500]},
            {"type": "cadence", "data": [80, 81, 82, 83, 84]}
        ]);
        let window = StreamWindow {
            window_type: Some("elapsed_time".to_string()),
            start: "00:01:00".to_string(),
            end: "00:04:00".to_string(),
        };

        let result = transform_streams_with_window(
            input,
            None,
            true,
            Some(vec!["watts".into()]),
            Some(&window),
        )
        .expect("window transform should succeed");

        assert!(result.get("time").is_none());
        assert!(result.get("cadence").is_none());
        assert_eq!(result["window"]["start_index"], 1);
        assert_eq!(result["window"]["end_index"], 4);
        assert_eq!(result["window"]["points"], 3);
        assert_eq!(result["watts"]["count"], 3);
        assert_eq!(result["watts"]["avg"], 300.0);
        assert_eq!(result["watts"]["p90"], 400.0);
    }

    #[test]
    fn transform_streams_window_accepts_power_alias_for_watts() {
        let input = serde_json::json!([
            {"type": "time", "data": [0, 60, 120]},
            {"type": "watts", "data": [100, 200, 300]},
            {"type": "cadence", "data": [80, 81, 82]}
        ]);
        let window = StreamWindow {
            window_type: None,
            start: "00:00".to_string(),
            end: "03:00".to_string(),
        };

        let result = transform_streams_with_window(
            input,
            None,
            true,
            Some(vec!["power".into()]),
            Some(&window),
        )
        .expect("power alias should match watts");

        assert!(result.get("cadence").is_none());
        assert_eq!(result["watts"]["count"], 3);
        assert!(result.get("power").is_none());
    }

    #[test]
    fn transform_streams_window_reports_clamped_end() {
        let input = serde_json::json!([
            {"type": "time", "data": [0, 60, 120]},
            {"type": "watts", "data": [100, 200, 300]}
        ]);
        let window = StreamWindow {
            window_type: None,
            start: "01:00".to_string(),
            end: "10:00".to_string(),
        };

        let result = transform_streams_with_window(
            input,
            None,
            false,
            Some(vec!["watts".into()]),
            Some(&window),
        )
        .expect("out-of-bounds end should clamp");

        assert_eq!(result["window"]["clamped"], true);
        assert_eq!(result["window"]["clamped_end"], true);
        assert_eq!(result["window"]["actual_start_seconds"], 60.0);
        assert_eq!(result["window"]["actual_end_seconds"], 120.0);
        assert_eq!(result["watts"], serde_json::json!([200, 300]));
    }

    #[test]
    fn transform_streams_window_slices_before_downsample() {
        let input = serde_json::json!([
            {"type": "time", "data": [0, 60, 120, 180, 240]},
            {"type": "watts", "data": [100, 200, 300, 400, 500]}
        ]);
        let window = StreamWindow {
            window_type: None,
            start: "01:00".to_string(),
            end: "04:00".to_string(),
        };

        let result = transform_streams_with_window(
            input,
            Some(2),
            false,
            Some(vec!["watts".into()]),
            Some(&window),
        )
        .expect("window transform should succeed");

        assert_eq!(result["watts"], serde_json::json!([200, 400]));
        assert_eq!(result["window"]["end_exclusive"], true);
    }

    #[test]
    fn transform_streams_window_handles_nested_object_streams() {
        let input = serde_json::json!({
            "id": "a1",
            "streams": {
                "time": [0, 1, 2, 3, 4],
                "watts": [10, 20, 30, 40, 50]
            }
        });
        let window = StreamWindow {
            window_type: Some("elapsed_time".to_string()),
            start: "1".to_string(),
            end: "4".to_string(),
        };

        let result = transform_streams_with_window(
            input,
            None,
            false,
            Some(vec!["watts".into()]),
            Some(&window),
        )
        .expect("window transform should succeed");

        assert_eq!(result["id"], "a1");
        assert_eq!(result["streams"]["watts"], serde_json::json!([20, 30, 40]));
        assert!(result["streams"].get("time").is_none());
        assert_eq!(result["streams"]["window"]["points"], 3);
    }

    #[test]
    fn transform_streams_window_rejects_invalid_range() {
        let input = serde_json::json!([
            {"type": "time", "data": [0, 60, 120]},
            {"type": "watts", "data": [100, 200, 300]}
        ]);
        let window = StreamWindow {
            window_type: Some("elapsed_time".to_string()),
            start: "02:00".to_string(),
            end: "01:00".to_string(),
        };

        let err = transform_streams_with_window(input, None, true, None, Some(&window))
            .expect_err("end before start should fail");
        assert!(err.contains("end must be after start"));
    }

    #[test]
    fn transform_streams_window_rejects_missing_time_stream() {
        let input = serde_json::json!([
            {"type": "watts", "data": [100, 200, 300]}
        ]);
        let window = StreamWindow {
            window_type: Some("ELAPSED_TIME".to_string()),
            start: "00:00:00".to_string(),
            end: "00:01:00".to_string(),
        };

        let err = transform_streams_with_window(input, None, true, None, Some(&window))
            .expect_err("window without time should fail");
        assert!(err.contains("time stream"));
    }

    #[test]
    fn transform_curves_compacts_parallel_array_payload() {
        let input = serde_json::json!({
            "list": [{
                "label": "42 days",
                "secs": [1, 5, 60, 300],
                "watts": [500, 450, 300, 250],
                "watts_per_kg": [8.1, 7.2, 4.8, 4.0],
                "weight": 62.0,
                "powerModels": [{"type": "FFT", "ftp": 220, "wPrime": 12000}],
                "vo2max_5m": 52.44
            }],
            "activities": {"a1": {}, "a2": {}}
        });

        let result = transform_curves(&input, true, None);
        assert_eq!(result["activities_count"], 2);
        assert_eq!(result["curves"][0]["key_powers"]["1s"]["watts"], 500);
        assert_eq!(result["curves"][0]["key_powers"]["300s"]["w_kg"], 4.0);
        assert_eq!(result["curves"][0]["ftp_estimates"][0]["w_prime"], 12000);
        assert_eq!(result["curves"][0]["vo2max"], 52.4);
    }

    #[test]
    fn transform_histogram_summary_understands_secs_bins() {
        let input = serde_json::json!([
            {"min": 0, "max": 99, "secs": 10},
            {"min": 100, "max": 199, "secs": 20}
        ]);

        let result = transform_histogram(&input, true, 10);
        assert_eq!(result["total_samples"], 30);
        assert_eq!(result["min"], 0.0);
        assert_eq!(result["max"], 199.0);
        assert_eq!(result["weighted_avg"], 116.17);
    }

    #[test]
    fn downsample_array_no_change_needed() {
        let arr = vec![
            serde_json::json!(1),
            serde_json::json!(2),
            serde_json::json!(3),
        ];
        let result = downsample_array(&arr, 5);
        assert_eq!(result, arr);
    }

    #[test]
    fn downsample_array_target_too_small() {
        let arr = vec![
            serde_json::json!(1),
            serde_json::json!(2),
            serde_json::json!(3),
        ];
        let result = downsample_array(&arr, 1);
        assert_eq!(result, arr);
    }

    #[test]
    fn downsample_array_basic_downsampling() {
        let arr = (0..10).map(|i| serde_json::json!(i)).collect::<Vec<_>>();
        let result = downsample_array(&arr, 4);
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], serde_json::json!(0));
        assert_eq!(result[3], serde_json::json!(9));
    }

    #[test]
    fn downsample_array_preserves_first_and_last() {
        let arr = vec![
            serde_json::json!("first"),
            serde_json::json!("middle"),
            serde_json::json!("last"),
        ];
        let result = downsample_array(&arr, 2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], serde_json::json!("first"));
        assert_eq!(result[1], serde_json::json!("last"));
    }
}
