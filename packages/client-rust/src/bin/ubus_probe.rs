//! ubus 协议探针：在音箱上直接验证「原生 socket 调用 mediaplayer 状态」是否可行。
//!
//! 用法: ubus_probe [socket路径，默认 /var/run/ubus.sock]

use ubus::client::UbusClient;
use ubus::unix::UnixUbusTransport;
use ubus::{json::blobmsg_to_json, BlobMsgTable};

fn main() {
    let sock = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/var/run/ubus.sock".to_string());
    println!("🚗 连接 ubus: {sock}");

    let transport = UnixUbusTransport::connect(&sock).expect("❌ 连接 ubus socket 失败");
    println!("✅ socket 已连接");

    let mut client = UbusClient::connect(transport).expect("❌ ubus HELLO 失败");
    println!("✅ HELLO 成功");

    let obj = client
        .lookup("mediaplayer")
        .expect("❌ lookup mediaplayer 失败");
    println!("✅ lookup 成功: mediaplayer = {obj:?}");

    // 连续 invoke 20 次计时，验证零 spawn 的延迟
    let t0 = std::time::Instant::now();
    let mut last_json = String::new();
    for i in 0..20 {
        let reply = client
            .invoke(obj, "player_get_play_status", &BlobMsgTable::empty())
            .expect("❌ invoke 失败");
        last_json = blobmsg_to_json(&reply)
            .expect("❌ blobmsg 转 JSON 失败")
            .to_string();
        if i == 0 {
            println!("✅ invoke 成功，原始回复 JSON: {last_json}");
        }
    }
    let elapsed = t0.elapsed();
    println!("⏱️ 20 次 invoke 耗时: {elapsed:?}（均摊 {:?}）", elapsed / 20);
    println!("最后一次: {last_json}");
}

