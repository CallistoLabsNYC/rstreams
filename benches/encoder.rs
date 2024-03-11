use bytes::BytesMut;
use criterion::*;
use samsa::{codecs::ToByte, protocol};

fn criterion_benchmark(c: &mut Criterion) {
    let correlation_id = 2;
    let client_id = "rust";
    let topic_name = "purchases";
    let partition_id = 3;

    let mut produce_req = protocol::ProduceRequest::new(0, 1000, correlation_id, client_id);
    produce_req.add(
        topic_name,
        partition_id,
        Some(&"Tester".as_bytes()),
        Some(&"Value 1".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id,
        Some(&"Tester".as_bytes()),
        Some(&"Value 2".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id + 1,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id + 1,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id + 1,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id + 1,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id + 1,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id + 1,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id + 1,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id + 1,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id + 1,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id + 1,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id + 1,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id + 1,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id + 1,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id + 1,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id + 1,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id + 1,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    produce_req.add(
        topic_name,
        partition_id + 1,
        Some(&"Tester".as_bytes()),
        Some(&"Value 3".as_bytes()),
    );
    let mut buffer = Vec::with_capacity(4);
    produce_req.encode(&mut buffer).unwrap();

    let mut encoder_group = c.benchmark_group("encoder");

    // is this right?
    encoder_group.throughput(Throughput::Bytes(buffer.len() as u64));
    encoder_group.bench_with_input(
        BenchmarkId::new("encode", buffer.len()),
        &produce_req,
        |b, data| {
            // trying to figure out how this works
            // does giving it too small of a vector make it work harder
            // to alloc more data?
            let mut buff = BytesMut::with_capacity(buffer.len());
            b.iter(|| data.encode(&mut buff).unwrap());
        },
    );

    encoder_group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
