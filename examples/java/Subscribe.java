// Subscribe to nats-lens violation events from Java
// Dependency (Maven): io.nats:jnats:2.17.0
import io.nats.client.*;
import io.nats.client.api.*;
import org.json.*;

public class Subscribe {
    public static void main(String[] args) throws Exception {
        Options opts = new Options.Builder()
            .server("nats://localhost:4222")
            .build();

        try (Connection nc = Nats.connect(opts)) {
            Dispatcher d = nc.createDispatcher(msg -> {
                try {
                    JSONObject event     = new JSONObject(new String(msg.getData()));
                    JSONObject violation = event.getJSONObject("violation");
                    String type     = violation.getString("type");
                    String stream   = event.getString("stream_name");
                    String consumer = event.getString("consumer_name");
                    String severity = event.getString("severity");
                    String fix      = violation.optString("fix_command", "see dashboard");

                    System.out.printf("[%s] %s on %s/%s%n", severity, type, stream, consumer);
                    System.out.printf("  Fix: %s%n", fix);
                } catch (Exception e) {
                    System.err.println("parse error: " + e.getMessage());
                }
            });

            d.subscribe("nats.lens.health.violations.>");
            System.out.println("Listening for nats-lens violations...");
            Thread.sleep(Long.MAX_VALUE);
        }
    }
}
