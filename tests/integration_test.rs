mod integration_test {
    use claim::assert_ok;
    use samsa::{network, protocol};

    #[test]
    fn test_index() {
        assert_eq!(1, 1);
    }

    #[ignore]
    #[tokio::test]
    async fn test_join_group_request() {
        let bootstrap_url = "127.0.0.1:9092,127.0.0.1:9093";
        let correlation_id = 1;
        let client_id = "rust";
        let consumer_group_key = "services2";
        let member_id = "";
        let protocol_type = "consumer-range";
        let topics = vec!["Some metadata"];
        let protocol = protocol::join_group::request::Protocol::new("consumer", topics);
        let protocols = vec![protocol];

        let coordinator_url = find_active_coordinator(
            bootstrap_url,
            correlation_id,
            client_id,
            consumer_group_key,
        )
        .await;
        assert!(coordinator_url.is_some());
        let coordinator_url = coordinator_url.unwrap();

        let conn = network::BrokerConnection::new(&coordinator_url).await;
        assert!(conn.is_ok());
        let conn = conn.unwrap();

        let req = protocol::join_group::request::JoinGroupRequest::new(
            correlation_id,
            client_id,
            consumer_group_key,
            30000,
            30000,
            member_id,
            protocol_type,
            protocols,
        );

        if let Err(e) = conn.send_request(&req).await {
            tracing::error!("{:?}", e);
            return;
        };

        let response = conn.receive_response().await;
        assert_ok!(&response);
        let res = response.unwrap();
        let a_joingroup_response = protocol::JoinGroupResponse::try_from(res.freeze()).unwrap();
        tracing::info!("{:?}", a_joingroup_response);
        assert_eq!(
            a_joingroup_response.error_code, 0,
            "failed with wrong error code"
        ); //success
        assert_ne!(
            a_joingroup_response.member_id.is_empty(),
            true,
            "Member id not populated"
        );
    }

    async fn find_active_coordinator(
        bootstrap_url: &str,
        correlation_id: i32,
        client_id: &str,
        consumer_group_key: &str,
    ) -> Option<String> {
        let conn = match network::BrokerConnection::new(bootstrap_url).await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::error!("{:?}", e);
                return None;
            }
        };

        let req = protocol::find_coordinator::request::FindCoordinatorRequest::new(
            correlation_id,
            client_id,
            consumer_group_key,
        );

        if let Err(e) = conn.send_request(&req).await {
            tracing::error!("{:?}", e);
            return None;
        };

        let coordinator_response = conn.receive_response().await.unwrap();
        let coordinator_response: protocol::FindCoordinatorResponse =
            (coordinator_response.freeze()).try_into().unwrap();

        let coordinator_host = String::from_utf8(coordinator_response.host.to_vec()).unwrap();
        let coordinator_port = coordinator_response.port;
        let coordinator_url = format!("{}:{}", coordinator_host, coordinator_port);
        Some(coordinator_url)
    }
}
