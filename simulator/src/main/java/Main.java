import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class Main {

    public static void main(String[] args) {
        byte[] bytes = new byte[1024 * 1024 * 1024];
        long startNanos = System.nanoTime();
        long buggers = 0;
        int loopCount = 100;
        for (int l = 0; l < loopCount; l++) {
            for (int k = 0; k < bytes.length; k += 8) {
                buggers += bytes[k];
            }
        }
        System.out.println(buggers);
        long durationNanos = System.nanoTime() - startNanos;

        long bytesTransported = loopCount * (long) bytes.length;

        System.out.println("durationNanos:" + durationNanos);
        System.out.println("durationSeconds:" + NANOSECONDS.toSeconds(durationNanos));
        System.out.println("bytes transported:" + bytesTransported);
        long bytesTransportedPerSecond = bytesTransported / NANOSECONDS.toSeconds(durationNanos);
        System.out.println("bytes transported/second:" + bytesTransportedPerSecond);
        System.out.println("gigabytes transported/second:" + bytesTransportedPerSecond / (1024 * 1024 * 1024));
    }
}
