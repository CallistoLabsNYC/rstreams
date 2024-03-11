mod metadata_protocol {
    use samsa::{network, protocol};

    const BOOTSTRAP_URL: &str = "127.0.0.1:9092";
    const CORRELATION_ID: i32 = 1;
    const CLIENT_ID: &str = "metadata integration test";


    #[ignore]
    #[tokio::test]
    async fn it_can_get_metadata() {
        let topics = ["purchases"];
        let conn = network::BrokerConnection::new(BOOTSTRAP_URL).await;
        assert!(conn.is_ok());
        let conn = conn.unwrap();

        let metadata_request = protocol::MetadataRequest::new(CORRELATION_ID, CLIENT_ID, &topics);
        let request = conn.send_request(&metadata_request).await;
        assert!(request.is_ok());
        
        let metadata_response = conn.receive_response().await;
        assert!(metadata_response.is_ok());

        let metadata = protocol::MetadataResponse::try_from(metadata_response.unwrap().freeze());
        assert!(metadata.is_ok());

        let metadata = metadata.unwrap();

        assert_eq!(metadata.brokers.len(), 2);
        assert_eq!(metadata.topics.len(), 1);

        assert_eq!(metadata.topics[0].name, bytes::Bytes::from_static(b"purchases"));
        assert_eq!(metadata.topics[0].partitions.len(), 4);
    }
}