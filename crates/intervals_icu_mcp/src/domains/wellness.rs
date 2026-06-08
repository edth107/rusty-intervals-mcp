//! Domain module for wellness data management.
//!
//! This module handles wellness data transformation and summarization.
//! It uses the `crate::compact` utilities for token-efficient JSON responses.
//!
//! # GRASP Principles
//! - **Information Expert**: Wellness summarization logic is encapsulated here
//! - **Low Coupling**: Uses centralized compact utilities

use serde_json::Value;

const FITNESS_KEYS: &[&str] = &[
    "id",
    "ctl",
    "ctlLoad",
    "atl",
    "atlLoad",
    "rampRate",
    "sportInfo",
    "hrv",
    "restingHR",
    "sleepScore",
    "sleepSecs",
    "sleepQuality",
    "weight",
    "bodyFat",
    "vo2max",
    "steps",
];

/// Default fields for wellness entries in compact responses.
///
/// This constant is used by both the compaction functions and can be
/// referenced by implementations of the `Compact` trait for wellness types.
pub const DEFAULT_FIELDS: &[&str] = &[
    "id",
    "sleepSecs",
    "stress",
    "restingHR",
    "hrv",
    "weight",
    "fatigue",
    "motivation",
];

/// Normalize a date string to YYYY-MM-DD format.
///
/// Accepts either YYYY-MM-DD or ISO 8601 datetimes.
/// This follows the **Information Expert** principle by keeping
/// date normalization logic in the domain module.
pub fn normalize_date(date_str: &str) -> Option<String> {
    crate::transforms::normalize_date_str(date_str)
}

pub fn transform_wellness(value: &Value, summary_only: bool, fields: Option<&[String]>) -> Value {
    let Some(arr) = value.as_array() else {
        return value.clone();
    };

    if summary_only {
        if let Some(summary) = transform_fitness_summary(arr) {
            return summary;
        }

        let mut sleep_total: f64 = 0.0;
        let mut stress_total: f64 = 0.0;
        let mut hr_total: f64 = 0.0;
        let mut hrv_total: f64 = 0.0;
        let mut sleep_count: usize = 0;
        let mut stress_count: usize = 0;
        let mut hr_count: usize = 0;
        let mut hrv_count: usize = 0;

        for item in arr {
            if let Some(obj) = item.as_object() {
                if let Some(v) = obj.get("sleepSecs").and_then(|v| v.as_f64()) {
                    sleep_total += v / 3600.0;
                    sleep_count += 1;
                }
                if let Some(v) = obj.get("stress").and_then(|v| v.as_f64()) {
                    stress_total += v;
                    stress_count += 1;
                }
                if let Some(v) = obj.get("restingHR").and_then(|v| v.as_f64()) {
                    hr_total += v;
                    hr_count += 1;
                }
                if let Some(v) = obj.get("hrv").and_then(|v| v.as_f64()) {
                    hrv_total += v;
                    hrv_count += 1;
                }
            }
        }

        return serde_json::json!({
            "days": arr.len(),
            "avg_sleep_hours": if sleep_count > 0 { (sleep_total / sleep_count as f64 * 10.0).round() / 10.0 } else { 0.0 },
            "avg_stress": if stress_count > 0 { (stress_total / stress_count as f64 * 10.0).round() / 10.0 } else { 0.0 },
            "avg_resting_hr": if hr_count > 0 { (hr_total / hr_count as f64).round() } else { 0.0 },
            "avg_hrv": if hrv_count > 0 { (hrv_total / hrv_count as f64).round() } else { 0.0 }
        });
    }

    if let Some(field_list) = fields {
        let fields_to_use = if field_list.is_empty() {
            DEFAULT_FIELDS.to_vec()
        } else {
            field_list.iter().map(|s| s.as_str()).collect()
        };

        return crate::compact::compact_array(value, &fields_to_use, None, None);
    }

    value.clone()
}

fn transform_fitness_summary(arr: &[Value]) -> Option<Value> {
    let mut days = Vec::new();

    for day in arr {
        let obj = day.as_object()?;
        let mut compact_day = serde_json::Map::new();
        for key in FITNESS_KEYS {
            if let Some(value) = obj.get(*key).filter(|value| !value.is_null()) {
                compact_day.insert((*key).to_string(), value.clone());
            }
        }
        if !compact_day.is_empty() {
            days.push(Value::Object(compact_day));
        }
    }

    if days.is_empty() || !days[0].get("ctl").is_some() {
        return None;
    }

    let first = days.first()?.as_object()?;
    let last = days.last()?.as_object()?;

    let mut summary = serde_json::Map::new();
    summary.insert(
        "period".to_string(),
        Value::String(format!(
            "{} to {}",
            first.get("id").and_then(Value::as_str).unwrap_or("?"),
            last.get("id").and_then(Value::as_str).unwrap_or("?")
        )),
    );
    summary.insert("days".to_string(), Value::from(days.len()));

    if let (Some(first_ctl), Some(last_ctl)) = (
        first.get("ctl").and_then(Value::as_f64),
        last.get("ctl").and_then(Value::as_f64),
    ) {
        summary.insert(
            "ctl_trend".to_string(),
            Value::String(format!(
                "{} \u{2192} {}",
                round_to(first_ctl, 1),
                round_to(last_ctl, 1)
            )),
        );
    }
    if let (Some(first_atl), Some(last_atl)) = (
        first.get("atl").and_then(Value::as_f64),
        last.get("atl").and_then(Value::as_f64),
    ) {
        summary.insert(
            "atl_trend".to_string(),
            Value::String(format!(
                "{} \u{2192} {}",
                round_to(first_atl, 1),
                round_to(last_atl, 1)
            )),
        );
    }
    if let (Some(ctl), Some(atl)) = (
        last.get("ctl").and_then(Value::as_f64),
        last.get("atl").and_then(Value::as_f64),
    ) {
        summary.insert(
            "tsb_current".to_string(),
            Value::from(round_to(ctl - atl, 1)),
        );
    }
    if let Some(ramp_rate) = last.get("rampRate").and_then(Value::as_f64) {
        summary.insert("ramp_rate".to_string(), Value::from(round_to(ramp_rate, 2)));
    }
    if let Some(eftp) = last
        .get("sportInfo")
        .and_then(Value::as_array)
        .and_then(|sport_info| sport_info.first())
        .and_then(|sport| sport.get("eftp"))
        .and_then(Value::as_f64)
    {
        summary.insert("eftp".to_string(), Value::from(round_to(eftp, 1)));
    }

    Some(serde_json::json!({ "summary": summary }))
}

pub fn compact_wellness_entry(value: &Value, fields: Option<&[String]>) -> Value {
    crate::compact::compact_object(value, DEFAULT_FIELDS, fields)
}

fn round_to(value: f64, decimals: u32) -> f64 {
    let factor = 10_f64.powi(decimals as i32);
    (value * factor).round() / factor
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transform_wellness_summary_returns_aggregates() {
        let input = serde_json::json!([
            {"sleepSecs": 28800, "stress": 20, "restingHR": 50, "hrv": 45},
            {"sleepSecs": 25200, "stress": 30, "restingHR": 55, "hrv": 40}
        ]);

        let out = transform_wellness(&input, true, None);
        assert_eq!(out["days"], 2);
        assert_eq!(out["avg_sleep_hours"], 7.5);
    }

    #[test]
    fn transform_wellness_summary_returns_fitness_trends_when_ctl_is_present() {
        let input = serde_json::json!([
            {"id": "2026-06-01", "ctl": 42.1, "atl": 53.7, "rampRate": -0.5},
            {"id": "2026-06-08", "ctl": 40.9, "atl": 40.5, "rampRate": -1.223, "sportInfo": [{"eftp": 226.34}]}
        ]);

        let out = transform_wellness(&input, true, None);
        assert_eq!(out["summary"]["period"], "2026-06-01 to 2026-06-08");
        assert_eq!(out["summary"]["ctl_trend"], "42.1 \u{2192} 40.9");
        assert_eq!(out["summary"]["atl_trend"], "53.7 \u{2192} 40.5");
        assert_eq!(out["summary"]["tsb_current"], 0.4);
        assert_eq!(out["summary"]["ramp_rate"], -1.22);
        assert_eq!(out["summary"]["eftp"], 226.3);
    }

    #[test]
    fn compact_wellness_entry_filters_fields() {
        let input = serde_json::json!({"id":"w1","sleepSecs":28800,"extra":"x"});
        let out = compact_wellness_entry(&input, Some(&["id".into()]));
        assert_eq!(out["id"], "w1");
        assert!(out.get("sleepSecs").is_none());
    }
}
