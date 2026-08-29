use intervals_icu_client::http_client::ReqwestIntervalsClient;
use intervals_icu_client::{ActivityIntervalInput, ActivityIntervalType, IntervalsClient};
use secrecy::SecretString;
use wiremock::matchers::{body_json, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn update_activity_intervals_always_merges_with_all_false() {
    let mock_server = MockServer::start().await;
    let intervals = vec![ActivityIntervalInput {
        start_index: 120,
        end_index: 480,
        label: "KRM Horn".into(),
        interval_type: ActivityIntervalType::Work,
    }];

    Mock::given(method("PUT"))
        .and(path("/api/v1/activity/i123/intervals"))
        .and(query_param("all", "false"))
        .and(body_json(serde_json::json!([{
            "start_index": 120,
            "end_index": 480,
            "label": "KRM Horn",
            "type": "WORK"
        }])))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "icu_intervals": [{
                "start_index": 120,
                "end_index": 480,
                "label": "KRM Horn",
                "type": "WORK"
            }]
        })))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = ReqwestIntervalsClient::new(
        &mock_server.uri(),
        "athlete",
        SecretString::new("key".into()),
    );
    let response = client
        .update_activity_intervals("i123", &intervals)
        .await
        .expect("interval update");

    assert_eq!(response["icu_intervals"][0]["label"], "KRM Horn");
}

#[test]
fn interval_validation_rejects_invalid_boundaries_and_labels() {
    let invalid_bounds = ActivityIntervalInput {
        start_index: 10,
        end_index: 10,
        label: "test".into(),
        interval_type: ActivityIntervalType::Work,
    };
    assert!(invalid_bounds.validate().is_err());

    let invalid_label = ActivityIntervalInput {
        start_index: 10,
        end_index: 20,
        label: "   ".into(),
        interval_type: ActivityIntervalType::Recovery,
    };
    assert!(invalid_label.validate().is_err());
}

#[tokio::test]
async fn update_activity_intervals_surfaces_api_errors() {
    let mock_server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/api/v1/activity/i123/intervals"))
        .and(query_param("all", "false"))
        .respond_with(ResponseTemplate::new(422).set_body_string("invalid interval"))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = ReqwestIntervalsClient::new(
        &mock_server.uri(),
        "athlete",
        SecretString::new("key".into()),
    );
    let intervals = vec![ActivityIntervalInput {
        start_index: 120,
        end_index: 480,
        label: "KRM Horn".into(),
        interval_type: ActivityIntervalType::Work,
    }];

    let error = client
        .update_activity_intervals("i123", &intervals)
        .await
        .expect_err("422 must be returned");
    assert!(error.to_string().contains("invalid interval"));
}
