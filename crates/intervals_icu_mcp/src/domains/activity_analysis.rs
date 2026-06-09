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

use crate::types::{ActivityPowerZoneStatsWindow, StreamWindow};

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

#[derive(Debug, Clone)]
struct PowerZoneDefinition {
    index: usize,
    zone: String,
    label: String,
    min_percent_ftp: f64,
    max_percent_ftp: f64,
    min_watts: i64,
    max_watts: Option<i64>,
}

#[derive(Debug, Clone)]
struct WeightedSample {
    index: usize,
    seconds: f64,
}

#[derive(Debug, Clone)]
struct WeightedValue {
    value: f64,
    seconds: f64,
}

#[derive(Debug)]
struct ZoneBounds {
    values: Vec<f64>,
    source: &'static str,
    model: &'static str,
}

#[derive(Debug)]
struct IncludeDecision {
    mask: Option<Vec<bool>>,
    analysis_time_basis: &'static str,
    include_filter: &'static str,
    movement_seconds: Option<f64>,
    basis_matches_moving_time: Option<bool>,
    basis_delta_seconds: Option<f64>,
}

#[derive(Debug)]
struct MovementBasis {
    mask: Option<Vec<bool>>,
    raw_mask: Option<Vec<bool>>,
    analysis_time_basis: &'static str,
    movement_seconds: Option<f64>,
    basis_matches_moving_time: Option<bool>,
    basis_delta_seconds: Option<f64>,
}

#[derive(Debug)]
struct AppliedPowerZoneWindow {
    mask: Vec<bool>,
    metadata: Value,
    elapsed_seconds: f64,
}

pub fn activity_power_zone_stats(
    activity_id: &str,
    details: &Value,
    streams: &Value,
    min_response_seconds: usize,
    zone_bounds_override: Option<&[f64]>,
    zone_labels_override: Option<&[String]>,
    window: Option<&ActivityPowerZoneStatsWindow>,
) -> Result<Value, String> {
    let effective_ftp = extract_effective_ftp(details)
        .ok_or_else(|| "activity power-zone stats require an activity FTP".to_string())?;
    if effective_ftp <= 0.0 {
        return Err("activity power-zone stats require a positive FTP".to_string());
    }

    let zone_bounds = resolve_power_zone_bounds(details, zone_bounds_override)?;

    let watts = stream_data(streams, "watts")
        .ok_or_else(|| "activity power-zone stats require a watts stream".to_string())?;
    let time = stream_data(streams, "time");
    let heartrate = stream_data(streams, "heartrate");
    let cadence = stream_data(streams, "cadence");
    let torque = stream_data(streams, "torque");
    let velocity_smooth = stream_data(streams, "velocity_smooth");
    let distance = stream_data(streams, "distance");

    let sample_weights = sample_second_weights(details, time, watts.len());
    let movement_basis = movement_basis(details, velocity_smooth, &sample_weights);
    let applied_window = power_zone_window_mask(
        window,
        time,
        distance,
        &sample_weights,
        movement_basis.mask.as_deref(),
        movement_basis.mask.is_some(),
    )?;
    let window_mask = applied_window.as_ref().map(|window| window.mask.as_slice());
    let include_decision =
        included_power_zone_sample_mask(&movement_basis, &sample_weights, window_mask);
    let total_elapsed_seconds = applied_window
        .as_ref()
        .map(|window| window.elapsed_seconds)
        .unwrap_or_else(|| total_elapsed_seconds(details, &sample_weights, watts.len()));
    let zones = build_power_zone_definitions(
        details,
        effective_ftp,
        &zone_bounds.values,
        zone_labels_override,
    );
    let mut samples_by_zone: Vec<Vec<WeightedSample>> = vec![Vec::new(); zones.len()];

    for (index, value) in watts.iter().enumerate() {
        if let Some(mask) = &include_decision.mask
            && !mask.get(index).copied().unwrap_or(false)
        {
            continue;
        }

        let seconds = sample_weights.get(index).copied().unwrap_or(1.0);
        if seconds <= 0.0 {
            continue;
        }

        let Some(watts_value) = numeric_value(value) else {
            continue;
        };
        let zone_index = zone_index_for_watts(watts_value, effective_ftp, &zone_bounds.values);
        if let Some(samples) = samples_by_zone.get_mut(zone_index) {
            samples.push(WeightedSample { index, seconds });
        }
    }

    let included_seconds_value: f64 = samples_by_zone
        .iter()
        .flatten()
        .map(|sample| sample.seconds)
        .sum();
    let included_seconds = rounded_seconds(included_seconds_value);
    let excluded_seconds =
        rounded_seconds((total_elapsed_seconds - included_seconds_value).max(0.0));
    let zone_values: Vec<Value> = zones
        .iter()
        .map(|zone| {
            let response_samples = samples_by_zone
                .get(zone.index)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let seconds_value: f64 = response_samples.iter().map(|sample| sample.seconds).sum();
            let seconds = rounded_seconds(seconds_value);
            let mut zone_obj = Map::new();
            zone_obj.insert("zone".to_string(), Value::String(zone.zone.clone()));
            zone_obj.insert("label".to_string(), Value::String(zone.label.clone()));
            insert_number(&mut zone_obj, "min_percent_ftp", zone.min_percent_ftp);
            insert_number(&mut zone_obj, "max_percent_ftp", zone.max_percent_ftp);
            zone_obj.insert("min_watts".to_string(), Value::from(zone.min_watts));
            zone_obj.insert(
                "max_watts".to_string(),
                zone.max_watts.map_or(Value::Null, Value::from),
            );
            zone_obj.insert("seconds".to_string(), Value::from(seconds));
            insert_number(
                &mut zone_obj,
                "percent_included",
                percent(seconds_value, included_seconds_value),
            );
            zone_obj.insert(
                "response".to_string(),
                power_zone_response(
                    watts,
                    heartrate,
                    cadence,
                    torque,
                    response_samples,
                    min_response_seconds,
                ),
            );
            Value::Object(zone_obj)
        })
        .collect();

    let mut result = Map::new();
    result.insert(
        "activity_id".to_string(),
        Value::String(activity_id.to_string()),
    );
    result.insert("zone_type".to_string(), Value::String("power".to_string()));
    result.insert(
        "zone_model".to_string(),
        Value::String(zone_bounds.model.to_string()),
    );
    result.insert(
        "zone_source".to_string(),
        Value::String(zone_bounds.source.to_string()),
    );
    insert_number(&mut result, "effective_ftp", effective_ftp);
    result.insert(
        "total_elapsed_seconds".to_string(),
        Value::from(rounded_seconds(total_elapsed_seconds)),
    );
    result.insert(
        "moving_time_seconds".to_string(),
        moving_time_seconds(details)
            .map_or(Value::Null, |seconds| Value::from(rounded_seconds(seconds))),
    );
    result.insert(
        "analysis_time_basis".to_string(),
        Value::String(include_decision.analysis_time_basis.to_string()),
    );
    result.insert(
        "include_filter".to_string(),
        Value::String(include_decision.include_filter.to_string()),
    );
    result.insert(
        "basis_matches_moving_time".to_string(),
        include_decision
            .basis_matches_moving_time
            .map_or(Value::Null, Value::from),
    );
    result.insert(
        "basis_delta_seconds".to_string(),
        include_decision
            .basis_delta_seconds
            .map_or(Value::Null, |seconds| {
                Value::from(rounded_seconds_signed(seconds))
            }),
    );
    result.insert(
        "movement_seconds".to_string(),
        include_decision
            .movement_seconds
            .map_or(Value::Null, |seconds| Value::from(rounded_seconds(seconds))),
    );
    result.insert(
        "included_seconds".to_string(),
        Value::from(included_seconds),
    );
    result.insert(
        "excluded_seconds".to_string(),
        Value::from(excluded_seconds),
    );
    result.insert(
        "min_response_seconds".to_string(),
        Value::from(min_response_seconds),
    );
    if let Some(applied_window) = applied_window {
        result.insert("window".to_string(), applied_window.metadata);
    }
    result.insert("zones".to_string(), Value::Array(zone_values));

    Ok(Value::Object(result))
}

fn movement_basis(
    details: &Value,
    velocity_smooth: Option<&Vec<Value>>,
    sample_weights: &[f64],
) -> MovementBasis {
    let Some(velocity_smooth) = velocity_smooth else {
        return MovementBasis {
            mask: None,
            raw_mask: None,
            analysis_time_basis: "elapsed_stream_samples",
            movement_seconds: None,
            basis_matches_moving_time: moving_time_seconds(details).map(|_| false),
            basis_delta_seconds: None,
        };
    };

    let mut mask = vec![false; sample_weights.len()];
    let mut movement_seconds = 0.0;

    for (index, included) in mask.iter_mut().enumerate() {
        if velocity_smooth
            .get(index)
            .and_then(numeric_value)
            .is_some_and(|speed| speed > 0.0)
        {
            *included = true;
            movement_seconds += sample_weights.get(index).copied().unwrap_or(0.0);
        }
    }

    let stream_seconds: f64 = sample_weights.iter().sum();
    if let Some(moving_time) = moving_time_seconds(details) {
        let delta = movement_seconds - moving_time;
        let matches = seconds_close(movement_seconds, moving_time);
        if movement_seconds > 0.0 && movement_seconds < stream_seconds && matches {
            return MovementBasis {
                mask: Some(mask.clone()),
                raw_mask: Some(mask),
                analysis_time_basis: "moving_time",
                movement_seconds: Some(movement_seconds),
                basis_matches_moving_time: Some(true),
                basis_delta_seconds: Some(delta),
            };
        }
        return MovementBasis {
            mask: None,
            raw_mask: Some(mask),
            analysis_time_basis: "elapsed_stream_samples",
            movement_seconds: Some(movement_seconds),
            basis_matches_moving_time: Some(matches),
            basis_delta_seconds: Some(delta),
        };
    }

    if movement_seconds > 0.0 && movement_seconds < stream_seconds {
        return MovementBasis {
            mask: Some(mask.clone()),
            raw_mask: Some(mask),
            analysis_time_basis: "movement_stream",
            movement_seconds: Some(movement_seconds),
            basis_matches_moving_time: None,
            basis_delta_seconds: None,
        };
    }

    MovementBasis {
        mask: None,
        raw_mask: Some(mask),
        analysis_time_basis: "elapsed_stream_samples",
        movement_seconds: Some(movement_seconds),
        basis_matches_moving_time: None,
        basis_delta_seconds: None,
    }
}

fn included_power_zone_sample_mask(
    movement_basis: &MovementBasis,
    sample_weights: &[f64],
    window_mask: Option<&[bool]>,
) -> IncludeDecision {
    let mask = combine_include_masks(movement_basis.mask.as_deref(), window_mask);
    let movement_seconds = movement_basis
        .raw_mask
        .as_deref()
        .map(|movement_mask| scoped_mask_seconds(sample_weights, Some(movement_mask), window_mask));

    IncludeDecision {
        mask,
        analysis_time_basis: movement_basis.analysis_time_basis,
        include_filter: include_filter_name(movement_basis.mask.is_some(), window_mask.is_some()),
        movement_seconds: movement_seconds.or(movement_basis.movement_seconds),
        basis_matches_moving_time: movement_basis.basis_matches_moving_time,
        basis_delta_seconds: movement_basis.basis_delta_seconds,
    }
}

fn include_filter_name(has_movement_mask: bool, has_window_mask: bool) -> &'static str {
    match (has_movement_mask, has_window_mask) {
        (true, true) => "window_and_velocity_smooth_gt_0",
        (true, false) => "velocity_smooth_gt_0",
        (false, true) => "window",
        (false, false) => "none",
    }
}

fn combine_include_masks(
    movement_mask: Option<&[bool]>,
    window_mask: Option<&[bool]>,
) -> Option<Vec<bool>> {
    match (movement_mask, window_mask) {
        (Some(movement), Some(window)) => {
            let len = movement.len().max(window.len());
            Some(
                (0..len)
                    .map(|index| {
                        movement.get(index).copied().unwrap_or(false)
                            && window.get(index).copied().unwrap_or(false)
                    })
                    .collect(),
            )
        }
        (Some(mask), None) | (None, Some(mask)) => Some(mask.to_vec()),
        (None, None) => None,
    }
}

fn scoped_mask_seconds(
    sample_weights: &[f64],
    primary_mask: Option<&[bool]>,
    window_mask: Option<&[bool]>,
) -> f64 {
    sample_weights
        .iter()
        .enumerate()
        .filter(|(index, _)| {
            primary_mask
                .map(|mask| mask.get(*index).copied().unwrap_or(false))
                .unwrap_or(true)
                && window_mask
                    .map(|mask| mask.get(*index).copied().unwrap_or(false))
                    .unwrap_or(true)
        })
        .map(|(_, seconds)| *seconds)
        .sum()
}

fn power_zone_window_mask(
    window: Option<&ActivityPowerZoneStatsWindow>,
    time: Option<&Vec<Value>>,
    distance: Option<&Vec<Value>>,
    sample_weights: &[f64],
    movement_mask: Option<&[bool]>,
    movement_basis_available: bool,
) -> Result<Option<AppliedPowerZoneWindow>, String> {
    let Some(window) = window else {
        return Ok(None);
    };

    let window_type = window.window_type.as_deref().unwrap_or("elapsed_time");
    let applied = if window_type.eq_ignore_ascii_case("elapsed_time") {
        elapsed_power_zone_window(window, time, sample_weights)?
    } else if window_type.eq_ignore_ascii_case("moving_time") {
        moving_power_zone_window(
            window,
            sample_weights,
            movement_mask,
            movement_basis_available,
        )?
    } else if window_type.eq_ignore_ascii_case("distance") {
        distance_power_zone_window(window, distance, sample_weights)?
    } else {
        return Err(format!(
            "unsupported power-zone stats window type '{window_type}', expected elapsed_time, moving_time, or distance"
        ));
    };

    Ok(Some(applied))
}

fn elapsed_power_zone_window(
    window: &ActivityPowerZoneStatsWindow,
    time: Option<&Vec<Value>>,
    sample_weights: &[f64],
) -> Result<AppliedPowerZoneWindow, String> {
    let time_data = time
        .ok_or_else(|| "power-zone stats elapsed_time window requires a time stream".to_string())?;
    let start_seconds = parse_window_time_seconds(&window.start, "window.start")?;
    let end_seconds = parse_window_time_seconds(&window.end, "window.end")?;
    if end_seconds <= start_seconds {
        return Err("power-zone stats window end must be after start".to_string());
    }

    let start_index = first_index_at_or_after(time_data, start_seconds);
    let end_index = first_index_at_or_after(time_data, end_seconds).max(start_index);
    let mask = contiguous_window_mask(sample_weights.len(), start_index, end_index);
    let elapsed_seconds = scoped_mask_seconds(sample_weights, None, Some(&mask));
    let first_available_seconds = first_available_seconds(time_data);
    let last_available_seconds = last_available_seconds(time_data);
    let actual_start_seconds = seconds_at_index(time_data, start_index);
    let actual_end_seconds = seconds_at_index(time_data, end_index).or(last_available_seconds);
    let clamped_start = first_available_seconds.is_some_and(|first| start_seconds < first)
        || (actual_start_seconds.is_none() && start_index >= time_data.len());
    let clamped_end = last_available_seconds.is_some_and(|last| end_seconds > last);

    Ok(AppliedPowerZoneWindow {
        mask,
        elapsed_seconds,
        metadata: serde_json::json!({
            "type": "elapsed_time",
            "requested_start": window.start.clone(),
            "requested_end": window.end.clone(),
            "start_seconds": start_seconds,
            "end_seconds": end_seconds,
            "actual_start_seconds": actual_start_seconds,
            "actual_end_seconds": actual_end_seconds,
            "elapsed_seconds": rounded_seconds(elapsed_seconds),
            "clamped": clamped_start || clamped_end,
            "clamped_start": clamped_start,
            "clamped_end": clamped_end,
            "start_index": start_index,
            "end_index": end_index,
            "end_exclusive": true,
            "points": end_index.saturating_sub(start_index)
        }),
    })
}

fn moving_power_zone_window(
    window: &ActivityPowerZoneStatsWindow,
    sample_weights: &[f64],
    movement_mask: Option<&[bool]>,
    movement_basis_available: bool,
) -> Result<AppliedPowerZoneWindow, String> {
    if !movement_basis_available {
        return Err(
            "power-zone stats moving_time window requires a trusted velocity_smooth movement basis"
                .to_string(),
        );
    }
    let movement_mask = movement_mask.ok_or_else(|| {
        "power-zone stats moving_time window requires a trusted velocity_smooth movement basis"
            .to_string()
    })?;
    let start_seconds = parse_window_time_seconds(&window.start, "window.start")?;
    let end_seconds = parse_window_time_seconds(&window.end, "window.end")?;
    if end_seconds <= start_seconds {
        return Err("power-zone stats window end must be after start".to_string());
    }

    let mut cumulative = 0.0;
    let mut actual_start_seconds: Option<f64> = None;
    let mut actual_end_seconds: Option<f64> = None;
    let mut start_index: Option<usize> = None;
    let mut end_index: Option<usize> = None;
    let mut moving_points = 0usize;

    for (index, seconds) in sample_weights.iter().enumerate() {
        if !movement_mask.get(index).copied().unwrap_or(false) || *seconds <= 0.0 {
            continue;
        }

        let sample_start = cumulative;
        let sample_end = cumulative + *seconds;
        if sample_start >= start_seconds && sample_start < end_seconds {
            actual_start_seconds.get_or_insert(sample_start);
            actual_end_seconds = Some(sample_end);
            start_index.get_or_insert(index);
            end_index = Some(index + 1);
            moving_points += 1;
        }
        cumulative = sample_end;
    }

    let start_index_value = start_index.unwrap_or(sample_weights.len());
    let end_index_value = end_index.unwrap_or(start_index_value);
    let mask = contiguous_window_mask(sample_weights.len(), start_index_value, end_index_value);
    let elapsed_seconds = scoped_mask_seconds(sample_weights, None, Some(&mask));
    let moving_seconds = scoped_mask_seconds(sample_weights, Some(movement_mask), Some(&mask));
    let clamped_start = start_seconds > cumulative || actual_start_seconds.is_none();
    let clamped_end = end_seconds > cumulative;

    Ok(AppliedPowerZoneWindow {
        mask,
        elapsed_seconds,
        metadata: serde_json::json!({
            "type": "moving_time",
            "requested_start": window.start.clone(),
            "requested_end": window.end.clone(),
            "start_seconds": start_seconds,
            "end_seconds": end_seconds,
            "actual_start_seconds": actual_start_seconds,
            "actual_end_seconds": actual_end_seconds,
            "elapsed_seconds": rounded_seconds(elapsed_seconds),
            "moving_seconds": rounded_seconds(moving_seconds),
            "available_moving_seconds": rounded_seconds(cumulative),
            "clamped": clamped_start || clamped_end,
            "clamped_start": clamped_start,
            "clamped_end": clamped_end,
            "start_index": start_index_value,
            "end_index": end_index_value,
            "end_exclusive": true,
            "points": end_index_value.saturating_sub(start_index_value),
            "moving_points": moving_points
        }),
    })
}

fn distance_power_zone_window(
    window: &ActivityPowerZoneStatsWindow,
    distance: Option<&Vec<Value>>,
    sample_weights: &[f64],
) -> Result<AppliedPowerZoneWindow, String> {
    let distance_data = distance
        .ok_or_else(|| "power-zone stats distance window requires a distance stream".to_string())?;
    let start_meters = parse_window_number(&window.start, "window.start")?;
    let end_meters = parse_window_number(&window.end, "window.end")?;
    if end_meters <= start_meters {
        return Err("power-zone stats window end must be after start".to_string());
    }

    let start_index = first_numeric_index_at_or_after(distance_data, start_meters);
    let end_index = first_numeric_index_at_or_after(distance_data, end_meters).max(start_index);
    let mask = contiguous_window_mask(sample_weights.len(), start_index, end_index);
    let elapsed_seconds = scoped_mask_seconds(sample_weights, None, Some(&mask));
    let first_available_meters = first_available_number(distance_data);
    let last_available_meters = last_available_number(distance_data);
    let actual_start_meters = number_at_index(distance_data, start_index);
    let actual_end_meters = number_at_index(distance_data, end_index).or(last_available_meters);
    let clamped_start = first_available_meters.is_some_and(|first| start_meters < first)
        || (actual_start_meters.is_none() && start_index >= distance_data.len());
    let clamped_end = last_available_meters.is_some_and(|last| end_meters > last);

    Ok(AppliedPowerZoneWindow {
        mask,
        elapsed_seconds,
        metadata: serde_json::json!({
            "type": "distance",
            "requested_start": window.start.clone(),
            "requested_end": window.end.clone(),
            "start_meters": start_meters,
            "end_meters": end_meters,
            "actual_start_meters": actual_start_meters,
            "actual_end_meters": actual_end_meters,
            "elapsed_seconds": rounded_seconds(elapsed_seconds),
            "clamped": clamped_start || clamped_end,
            "clamped_start": clamped_start,
            "clamped_end": clamped_end,
            "start_index": start_index,
            "end_index": end_index,
            "end_exclusive": true,
            "points": end_index.saturating_sub(start_index)
        }),
    })
}

fn contiguous_window_mask(sample_count: usize, start_index: usize, end_index: usize) -> Vec<bool> {
    let start = start_index.min(sample_count);
    let end = end_index.min(sample_count).max(start);
    (0..sample_count)
        .map(|index| index >= start && index < end)
        .collect()
}

fn parse_window_time_seconds(value: &Value, field: &str) -> Result<f64, String> {
    if let Some(text) = value.as_str() {
        return parse_elapsed_seconds(text).map_err(|err| format!("{field}: {err}"));
    }
    parse_window_number(value, field)
}

fn parse_window_number(value: &Value, field: &str) -> Result<f64, String> {
    numeric_value(value)
        .filter(|number| number.is_finite() && *number >= 0.0)
        .ok_or_else(|| format!("{field} must be a non-negative finite number"))
}

fn first_numeric_index_at_or_after(data: &[Value], target: f64) -> usize {
    data.iter()
        .position(|value| numeric_value(value).is_some_and(|number| number >= target))
        .unwrap_or(data.len())
}

fn number_at_index(data: &[Value], index: usize) -> Option<f64> {
    data.get(index).and_then(numeric_value)
}

fn first_available_number(data: &[Value]) -> Option<f64> {
    data.iter().find_map(numeric_value)
}

fn last_available_number(data: &[Value]) -> Option<f64> {
    data.iter().rev().find_map(numeric_value)
}

fn moving_time_seconds(details: &Value) -> Option<f64> {
    details
        .as_object()
        .and_then(|obj| number_field(obj, &["moving_time"]))
        .map(|seconds| seconds.max(0.0))
}

fn seconds_close(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1.0
}

fn extract_effective_ftp(details: &Value) -> Option<f64> {
    let obj = details.as_object()?;
    number_field(obj, &["icu_ftp", "effective_ftp", "ftp", "indoor_ftp"])
}

fn resolve_power_zone_bounds(
    details: &Value,
    override_bounds: Option<&[f64]>,
) -> Result<ZoneBounds, String> {
    if let Some(bounds) = override_bounds {
        let values = bounds.to_vec();
        validate_power_zone_bounds(&values)?;
        return Ok(ZoneBounds {
            values,
            source: "custom.zone_bounds_percent",
            model: "custom_power_zones",
        });
    }

    extract_power_zone_bounds(details)
        .ok_or_else(|| "activity power-zone stats require power-zone bounds".to_string())
        .and_then(|bounds| {
            validate_power_zone_bounds(&bounds.values)?;
            Ok(bounds)
        })
}

fn validate_power_zone_bounds(bounds: &[f64]) -> Result<(), String> {
    if bounds.is_empty() {
        return Err("activity power-zone stats require at least one power zone".to_string());
    }
    let mut previous = 0.0;
    for (index, bound) in bounds.iter().enumerate() {
        if !bound.is_finite() || *bound <= 0.0 {
            return Err(format!(
                "zone_bounds_percent[{}] must be a positive finite number",
                index
            ));
        }
        if index > 0 && *bound <= previous {
            return Err("zone_bounds_percent must be strictly increasing".to_string());
        }
        previous = *bound;
    }
    if bounds.last().is_some_and(|bound| *bound < 999.0) {
        return Err(
            "zone_bounds_percent must end with an open-ended sentinel such as 999".to_string(),
        );
    }
    Ok(())
}

fn extract_power_zone_bounds(details: &Value) -> Option<ZoneBounds> {
    let obj = details.as_object()?;
    for (key, source) in [
        ("icu_power_zones", "activity.icu_power_zones"),
        ("power_zones", "activity.power_zones"),
    ] {
        if let Some(bounds) = obj.get(key).and_then(Value::as_array) {
            let values: Vec<f64> = bounds.iter().filter_map(numeric_value).collect();
            if !values.is_empty() {
                return Some(ZoneBounds {
                    values,
                    source,
                    model: "activity_power_zones",
                });
            }
        }
    }
    None
}

fn build_power_zone_definitions(
    details: &Value,
    effective_ftp: f64,
    zone_bounds: &[f64],
    labels_override: Option<&[String]>,
) -> Vec<PowerZoneDefinition> {
    let labels = power_zone_labels(details, zone_bounds.len(), labels_override);

    zone_bounds
        .iter()
        .enumerate()
        .map(|(index, max_percent)| {
            let min_percent = if index == 0 {
                0.0
            } else {
                zone_bounds[index - 1] + 1.0
            };
            PowerZoneDefinition {
                index,
                zone: format!("Z{}", index + 1),
                label: labels
                    .get(index)
                    .cloned()
                    .unwrap_or_else(|| format!("Z{}", index + 1)),
                min_percent_ftp: min_percent,
                max_percent_ftp: *max_percent,
                min_watts: if index == 0 {
                    0
                } else {
                    watts_for_percent(effective_ftp, min_percent)
                },
                max_watts: if *max_percent >= 999.0 {
                    None
                } else {
                    Some(watts_for_percent(effective_ftp, *max_percent))
                },
            }
        })
        .collect()
}

fn power_zone_labels(
    details: &Value,
    zone_count: usize,
    labels_override: Option<&[String]>,
) -> Vec<String> {
    const DEFAULT_LABELS: [&str; 7] = [
        "Active Recovery",
        "Endurance",
        "Tempo",
        "Sweet Spot",
        "Threshold",
        "VO2Max",
        "Anaerobic Capacity",
    ];

    let labels = labels_override
        .map(|values| values.to_vec())
        .or_else(|| {
            details
                .as_object()
                .and_then(|obj| {
                    obj.get("icu_power_zone_names")
                        .or_else(|| obj.get("power_zone_names"))
                })
                .and_then(Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(Value::as_str)
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                })
        })
        .unwrap_or_default();

    if labels.len() >= zone_count {
        return labels;
    }

    (0..zone_count)
        .map(|index| {
            labels.get(index).cloned().unwrap_or_else(|| {
                DEFAULT_LABELS
                    .get(index)
                    .unwrap_or(&"Power Zone")
                    .to_string()
            })
        })
        .collect()
}

fn stream_data<'a>(value: &'a Value, stream: &str) -> Option<&'a Vec<Value>> {
    match value {
        Value::Array(streams) if is_stream_list(streams) => streams
            .iter()
            .filter_map(Value::as_object)
            .find(|obj| {
                canonical_stream_name(stream_type_from_object(obj))
                    .eq_ignore_ascii_case(canonical_stream_name(stream))
            })
            .and_then(|obj| obj.get("data").and_then(Value::as_array)),
        Value::Object(obj) => {
            if let Some(streams) = obj.get("streams")
                && let Some(data) = stream_data(streams, stream)
            {
                return Some(data);
            }

            obj.iter()
                .find(|(key, _)| {
                    canonical_stream_name(key).eq_ignore_ascii_case(canonical_stream_name(stream))
                })
                .and_then(|(_, value)| value.as_array())
        }
        _ => None,
    }
}

fn zone_index_for_watts(watts: f64, effective_ftp: f64, zone_bounds: &[f64]) -> usize {
    let percent_ftp = watts / effective_ftp * 100.0;
    zone_bounds
        .iter()
        .position(|bound| percent_ftp <= *bound)
        .unwrap_or_else(|| zone_bounds.len().saturating_sub(1))
}

fn sample_second_weights(
    details: &Value,
    time: Option<&Vec<Value>>,
    sample_count: usize,
) -> Vec<f64> {
    let Some(time) = time else {
        return vec![1.0; sample_count];
    };
    let detail_elapsed = elapsed_time_seconds(details);
    (0..sample_count)
        .map(|index| {
            let Some(start) = time.get(index).and_then(numeric_value) else {
                return 1.0;
            };
            let end = time
                .get(index + 1)
                .and_then(numeric_value)
                .or_else(|| detail_elapsed.filter(|elapsed| *elapsed > start));
            end.map(|value| (value - start).max(0.0)).unwrap_or(0.0)
        })
        .collect()
}

fn total_elapsed_seconds(details: &Value, sample_weights: &[f64], fallback_samples: usize) -> f64 {
    elapsed_time_seconds(details).unwrap_or_else(|| {
        sample_weights
            .iter()
            .sum::<f64>()
            .max(fallback_samples as f64)
    })
}

fn elapsed_time_seconds(details: &Value) -> Option<f64> {
    details
        .as_object()
        .and_then(|obj| number_field(obj, &["elapsed_time", "total_elapsed_time", "moving_time"]))
        .map(|seconds| seconds.max(0.0))
}

fn power_zone_response(
    watts: &[Value],
    heartrate: Option<&Vec<Value>>,
    cadence: Option<&Vec<Value>>,
    torque: Option<&Vec<Value>>,
    samples: &[WeightedSample],
    min_response_seconds: usize,
) -> Value {
    let response_seconds_value: f64 = samples.iter().map(|sample| sample.seconds).sum();
    let response_seconds = rounded_seconds(response_seconds_value);
    let mut response = Map::new();
    response.insert(
        "valid".to_string(),
        Value::from(response_seconds_value >= min_response_seconds as f64),
    );
    response.insert("seconds".to_string(), Value::from(response_seconds));

    if response_seconds_value < min_response_seconds as f64 {
        response.insert(
            "reason".to_string(),
            Value::String("below_min_response_seconds".to_string()),
        );
        return Value::Object(response);
    }

    if let Some(stats) = power_response_stats(&collect_stream_values(Some(watts), samples)) {
        response.insert("power".to_string(), Value::Object(stats));
    }
    if let Some(stats) = heartrate_response_stats(&collect_stream_values(
        heartrate.map(Vec::as_slice),
        samples,
    )) {
        response.insert("heartrate".to_string(), Value::Object(stats));
    }
    if let Some(stats) =
        cadence_response_stats(&collect_stream_values(cadence.map(Vec::as_slice), samples))
    {
        response.insert("cadence".to_string(), Value::Object(stats));
    }
    if let Some(stats) =
        torque_response_stats(&collect_stream_values(torque.map(Vec::as_slice), samples))
    {
        response.insert("torque".to_string(), Value::Object(stats));
    }

    Value::Object(response)
}

fn collect_stream_values(
    stream: Option<&[Value]>,
    samples: &[WeightedSample],
) -> Vec<WeightedValue> {
    let Some(stream) = stream else {
        return Vec::new();
    };
    samples
        .iter()
        .filter_map(|sample| {
            stream
                .get(sample.index)
                .and_then(numeric_value)
                .filter(|_| sample.seconds > 0.0)
                .map(|value| WeightedValue {
                    value,
                    seconds: sample.seconds,
                })
        })
        .collect()
}

fn power_response_stats(values: &[WeightedValue]) -> Option<Map<String, Value>> {
    let mut stats = Map::new();
    insert_number(&mut stats, "avg", average(values)?);
    insert_number(&mut stats, "p50", percentile(values, 50)?);
    Some(stats)
}

fn heartrate_response_stats(values: &[WeightedValue]) -> Option<Map<String, Value>> {
    let mut stats = Map::new();
    insert_number(&mut stats, "avg", average(values)?);
    insert_number(&mut stats, "p90", percentile(values, 90)?);
    Some(stats)
}

fn cadence_response_stats(values: &[WeightedValue]) -> Option<Map<String, Value>> {
    let mut stats = Map::new();
    insert_number(&mut stats, "avg", average(values)?);
    insert_number(&mut stats, "min", min_value(values)?);
    Some(stats)
}

fn torque_response_stats(values: &[WeightedValue]) -> Option<Map<String, Value>> {
    let mut stats = Map::new();
    insert_number(&mut stats, "avg", average(values)?);
    insert_number(&mut stats, "max", max_value(values)?);
    Some(stats)
}

fn average(values: &[WeightedValue]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let seconds: f64 = values.iter().map(|value| value.seconds).sum();
    if seconds <= 0.0 {
        return None;
    }
    Some(
        values
            .iter()
            .map(|value| value.value * value.seconds)
            .sum::<f64>()
            / seconds,
    )
}

fn min_value(values: &[WeightedValue]) -> Option<f64> {
    values.iter().map(|value| value.value).reduce(f64::min)
}

fn max_value(values: &[WeightedValue]) -> Option<f64> {
    values.iter().map(|value| value.value).reduce(f64::max)
}

fn percentile(values: &[WeightedValue], percentile: usize) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| {
        a.value
            .partial_cmp(&b.value)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let total_seconds: f64 = sorted.iter().map(|value| value.seconds).sum();
    if total_seconds <= 0.0 {
        return None;
    }
    let target = total_seconds * percentile as f64 / 100.0;
    let mut cumulative = 0.0;
    for value in &sorted {
        cumulative += value.seconds;
        if cumulative >= target {
            return Some(value.value);
        }
    }
    sorted.last().map(|value| value.value)
}

fn watts_for_percent(effective_ftp: f64, percent: f64) -> i64 {
    (effective_ftp * percent / 100.0).round() as i64
}

fn percent(seconds: f64, total_seconds: f64) -> f64 {
    if total_seconds <= 0.0 {
        return 0.0;
    }
    seconds / total_seconds * 100.0
}

fn rounded_seconds(seconds: f64) -> i64 {
    seconds.round().max(0.0) as i64
}

fn rounded_seconds_signed(seconds: f64) -> i64 {
    seconds.round() as i64
}

fn insert_number(target: &mut Map<String, Value>, key: &str, value: f64) {
    let rounded = round_to(value, 2);
    if (rounded.fract()).abs() < f64::EPSILON {
        target.insert(key.to_string(), Value::from(rounded as i64));
    } else {
        target.insert(key.to_string(), Value::from(rounded));
    }
}

fn numeric_value(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|number| number as f64))
        .or_else(|| value.as_u64().map(|number| number as f64))
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
            "total_count": total,
            "source": source,
            "zone_type": "power",
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
    result.insert("zone_type".to_string(), Value::String("power".to_string()));
    result.insert("total_count".to_string(), Value::from(arr.len()));
    result.insert("returned_count".to_string(), Value::from(limited.len()));
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
        "end": format_elapsed_time(end_seconds),
        "end_exclusive": true
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
    fn activity_power_zone_stats_combines_distribution_and_response() {
        let details = serde_json::json!({
            "id": "i1",
            "icu_ftp": 100,
            "elapsed_time": 5,
            "moving_time": 3,
            "icu_power_zones": [54, 75, 87, 94, 105, 120, 999],
            "icu_zone_times": [
                {"id": "Z1", "secs": 2},
                {"id": "Z2", "secs": 0},
                {"id": "Z3", "secs": 0},
                {"id": "Z4", "secs": 0},
                {"id": "Z5", "secs": 1},
                {"id": "Z6", "secs": 0},
                {"id": "Z7", "secs": 0}
            ]
        });
        let streams = serde_json::json!({
            "streams": [
                {"type": "time", "data": [0, 1, 2, 3, 4]},
                {"type": "watts", "data": [50, 50, 100, 100, 100]},
                {"type": "heartrate", "data": [100, 102, 150, 160, 170]},
                {"type": "cadence", "data": [80, 82, 90, 91, 92]},
                {"type": "torque", "data": [10, 11, 30, 31, 32]},
                {"type": "velocity_smooth", "data": [2.1, 2.2, 2.3, null, null]}
            ]
        });

        let out = activity_power_zone_stats("i1", &details, &streams, 1, None, None, None).unwrap();
        assert_eq!(out["activity_id"], "i1");
        assert_eq!(out["effective_ftp"], 100);
        assert_eq!(out["total_elapsed_seconds"], 5);
        assert_eq!(out["analysis_time_basis"], "moving_time");
        assert_eq!(out["include_filter"], "velocity_smooth_gt_0");
        assert_eq!(out["basis_matches_moving_time"], true);
        assert_eq!(out["movement_seconds"], 3);
        assert_eq!(out["included_seconds"], 3);
        assert_eq!(out["excluded_seconds"], 2);

        let zones = out["zones"].as_array().unwrap();
        let z5 = zones.iter().find(|zone| zone["zone"] == "Z5").unwrap();
        assert_eq!(z5["label"], "Threshold");
        assert_eq!(z5["seconds"], 1);
        assert_eq!(z5["response"]["valid"], true);
        assert_eq!(z5["response"]["seconds"], 1);
        assert_eq!(z5["response"]["power"]["avg"], 100);
        assert_eq!(z5["response"]["heartrate"]["p90"], 150);
        assert_eq!(z5["response"]["cadence"]["min"], 90);
        assert_eq!(z5["response"]["torque"]["max"], 30);
        assert!(out.get("reference_zone_times").is_none());
        assert!(z5.get("intervals_icu_seconds").is_none());
        assert!(z5.get("delta_vs_intervals_icu_seconds").is_none());
        let zone_seconds: i64 = zones
            .iter()
            .map(|zone| zone["seconds"].as_i64().unwrap())
            .sum();
        let response_seconds: i64 = zones
            .iter()
            .map(|zone| zone["response"]["seconds"].as_i64().unwrap())
            .sum();
        assert_eq!(zone_seconds, response_seconds);
        assert_eq!(zone_seconds, out["included_seconds"].as_i64().unwrap());
        assert!(z5["response"].get("first_half").is_none());
        assert!(z5["response"].get("second_half").is_none());
    }

    #[test]
    fn activity_power_zone_stats_marks_short_response_invalid() {
        let details = serde_json::json!({
            "id": "i1",
            "icu_ftp": 100,
            "elapsed_time": 3,
            "icu_power_zones": [54, 75, 87, 94, 105, 120, 999],
            "icu_zone_times": [
                {"id": "Z5", "secs": 3}
            ]
        });
        let streams = serde_json::json!({
            "time": [0, 1, 2],
            "watts": [100, 100, 100],
            "heartrate": [150, 160, 170]
        });

        let out = activity_power_zone_stats("i1", &details, &streams, 4, None, None, None).unwrap();
        let z5 = out["zones"]
            .as_array()
            .unwrap()
            .iter()
            .find(|zone| zone["zone"] == "Z5")
            .unwrap();

        assert_eq!(z5["response"]["valid"], false);
        assert_eq!(z5["response"]["reason"], "below_min_response_seconds");
        assert_eq!(z5["response"]["seconds"], 3);
        assert!(z5["response"].get("power").is_none());
    }

    #[test]
    fn activity_power_zone_stats_accepts_custom_bounds_and_labels() {
        let details = serde_json::json!({
            "id": "i1",
            "icu_ftp": 100,
            "elapsed_time": 3,
            "icu_power_zones": [54, 75, 87, 94, 105, 120, 999]
        });
        let streams = serde_json::json!({
            "time": [0, 1, 2],
            "watts": [50, 80, 130]
        });
        let bounds = vec![50.0, 100.0, 999.0];
        let labels = vec!["Easy".to_string(), "Work".to_string(), "Hard".to_string()];

        let out = activity_power_zone_stats(
            "i1",
            &details,
            &streams,
            1,
            Some(&bounds),
            Some(&labels),
            None,
        )
        .unwrap();

        assert_eq!(out["zone_model"], "custom_power_zones");
        assert_eq!(out["zone_source"], "custom.zone_bounds_percent");
        let zones = out["zones"].as_array().unwrap();
        assert_eq!(zones[0]["label"], "Easy");
        assert_eq!(zones[1]["label"], "Work");
        assert_eq!(zones[2]["label"], "Hard");
        assert_eq!(zones[0]["seconds"], 1);
        assert_eq!(zones[1]["seconds"], 1);
        assert_eq!(zones[2]["seconds"], 1);
    }

    #[test]
    fn activity_power_zone_stats_rejects_non_increasing_custom_bounds() {
        let details = serde_json::json!({
            "id": "i1",
            "icu_ftp": 100,
            "elapsed_time": 1,
            "icu_power_zones": [54, 75, 87, 94, 105, 120, 999]
        });
        let streams = serde_json::json!({
            "time": [0],
            "watts": [50]
        });
        let bounds = vec![54.0, 54.0, 999.0];

        let err = activity_power_zone_stats("i1", &details, &streams, 1, Some(&bounds), None, None)
            .unwrap_err();

        assert!(err.contains("strictly increasing"));
    }

    #[test]
    fn activity_power_zone_stats_applies_elapsed_window_before_moving_mask() {
        let details = serde_json::json!({
            "id": "i1",
            "icu_ftp": 100,
            "elapsed_time": 6,
            "moving_time": 4,
            "icu_power_zones": [54, 75, 87, 94, 105, 120, 999]
        });
        let streams = serde_json::json!({
            "streams": [
                {"type": "time", "data": [0, 1, 2, 3, 4, 5]},
                {"type": "watts", "data": [50, 100, 100, 100, 100, 100]},
                {"type": "velocity_smooth", "data": [2.0, 2.0, 0.0, 2.0, 2.0, 0.0]}
            ]
        });
        let window = ActivityPowerZoneStatsWindow {
            window_type: Some("elapsed_time".to_string()),
            start: serde_json::json!(1),
            end: serde_json::json!(5),
        };

        let out = activity_power_zone_stats("i1", &details, &streams, 1, None, None, Some(&window))
            .unwrap();

        assert_eq!(out["window"]["type"], "elapsed_time");
        assert_eq!(out["window"]["start_index"], 1);
        assert_eq!(out["window"]["end_index"], 5);
        assert_eq!(out["total_elapsed_seconds"], 4);
        assert_eq!(out["included_seconds"], 3);
        assert_eq!(out["excluded_seconds"], 1);
        assert_eq!(out["include_filter"], "window_and_velocity_smooth_gt_0");
        assert_eq!(out["movement_seconds"], 3);
        let z5 = out["zones"]
            .as_array()
            .unwrap()
            .iter()
            .find(|zone| zone["zone"] == "Z5")
            .unwrap();
        assert_eq!(z5["seconds"], 3);
    }

    #[test]
    fn activity_power_zone_stats_supports_moving_time_window() {
        let details = serde_json::json!({
            "id": "i1",
            "icu_ftp": 100,
            "elapsed_time": 6,
            "moving_time": 4,
            "icu_power_zones": [54, 75, 87, 94, 105, 120, 999]
        });
        let streams = serde_json::json!({
            "streams": [
                {"type": "time", "data": [0, 1, 2, 3, 4, 5]},
                {"type": "watts", "data": [100, 100, 50, 100, 100, 50]},
                {"type": "velocity_smooth", "data": [2.0, 2.0, 0.0, 2.0, 2.0, 0.0]}
            ]
        });
        let window = ActivityPowerZoneStatsWindow {
            window_type: Some("moving_time".to_string()),
            start: serde_json::json!(1),
            end: serde_json::json!(3),
        };

        let out = activity_power_zone_stats("i1", &details, &streams, 1, None, None, Some(&window))
            .unwrap();

        assert_eq!(out["window"]["type"], "moving_time");
        assert_eq!(out["window"]["available_moving_seconds"], 4);
        assert_eq!(out["window"]["moving_seconds"], 2);
        assert_eq!(out["window"]["points"], 3);
        assert_eq!(out["window"]["moving_points"], 2);
        assert_eq!(out["total_elapsed_seconds"], 3);
        assert_eq!(out["included_seconds"], 2);
        assert_eq!(out["excluded_seconds"], 1);
        let z5 = out["zones"]
            .as_array()
            .unwrap()
            .iter()
            .find(|zone| zone["zone"] == "Z5")
            .unwrap();
        assert_eq!(z5["seconds"], 2);
    }

    #[test]
    fn activity_power_zone_stats_supports_distance_window() {
        let details = serde_json::json!({
            "id": "i1",
            "icu_ftp": 100,
            "elapsed_time": 6,
            "moving_time": 4,
            "icu_power_zones": [54, 75, 87, 94, 105, 120, 999]
        });
        let streams = serde_json::json!({
            "streams": [
                {"type": "time", "data": [0, 1, 2, 3, 4, 5]},
                {"type": "distance", "data": [0, 100, 200, 300, 400, 500]},
                {"type": "watts", "data": [50, 100, 100, 100, 100, 50]},
                {"type": "velocity_smooth", "data": [2.0, 2.0, 0.0, 2.0, 2.0, 0.0]}
            ]
        });
        let window = ActivityPowerZoneStatsWindow {
            window_type: Some("distance".to_string()),
            start: serde_json::json!(100),
            end: serde_json::json!(400),
        };

        let out = activity_power_zone_stats("i1", &details, &streams, 1, None, None, Some(&window))
            .unwrap();

        assert_eq!(out["window"]["type"], "distance");
        assert_eq!(out["window"]["start_index"], 1);
        assert_eq!(out["window"]["end_index"], 4);
        assert_eq!(out["total_elapsed_seconds"], 3);
        assert_eq!(out["included_seconds"], 2);
        assert_eq!(out["excluded_seconds"], 1);
        let z5 = out["zones"]
            .as_array()
            .unwrap()
            .iter()
            .find(|zone| zone["zone"] == "Z5")
            .unwrap();
        assert_eq!(z5["seconds"], 2);
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
