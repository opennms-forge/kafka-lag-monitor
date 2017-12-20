package org.opennms.kafka.lag;

public class HostAndPort {

    private String host;
    private int port;

    public final static HostAndPort fromString(final String hostWithPort) {

        int i = hostWithPort.lastIndexOf(":");
        if (i < 0 || (hostWithPort.length() == i)) {
            return null;
        }
        String[] hostWithPortArray = { hostWithPort.substring(0, i), hostWithPort.substring(i + 1) };

        HostAndPort hostAndPort = new HostAndPort();
        hostAndPort.setHost(hostWithPortArray[0]);
        hostAndPort.setPort(Integer.parseInt(hostWithPortArray[1]));
        return hostAndPort;

    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

}
