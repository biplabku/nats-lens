// Subscribe to nats-lens violation events from Node.js
// Install: npm install nats
// Run:     node subscribe.js
const { connect, StringCodec } = require("nats");

(async () => {
  const nc = await connect({ servers: "nats://localhost:4222" });
  const sc = StringCodec();

  const sub = nc.subscribe("nats.lens.health.violations.>");
  console.log("Listening for nats-lens violations...");

  for await (const msg of sub) {
    const event = JSON.parse(sc.decode(msg.data));
    const { type, fix_command } = event.violation;
    console.log(`[${event.severity}] ${type} on ${event.stream_name}/${event.consumer_name}`);
    console.log(`  Fix: ${fix_command}`);
  }
})();
