package se.afshin.yavari.kafka.operator.externalaccess;

import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;

/**
 * External-access sub-spec shared by HTTP services (KafkaUI, Apicurio Schema Registry).
 *
 * <ul>
 *   <li>{@code NODEPORT} (default): Service of type NodePort. {@code nodePort} can pin the port.
 *   <li>{@code LOADBALANCER}: Service of type LoadBalancer; advertised host is read from
 *       {@code status.loadBalancer.ingress[]} or substituted from {@code advertisedHostTemplate}.
 *   <li>{@code GATEWAY}: Service stays ClusterIP; a Gateway API HTTPRoute is attached to a
 *       parent Gateway named by {@link HttpGatewayConfig#getParentGatewayName()}.
 *   <li>{@code INGRESS}: Service stays ClusterIP; a {@code networking.k8s.io/v1 Ingress} is
 *       created. TLS termination is optional via {@link HttpIngressConfig#getTlsSecretRef()}.
 * </ul>
 */
public class HttpExternalAccessConfig implements ExternalAccessSpec {
    private ExternalAccessType type = ExternalAccessType.NODEPORT;
    private String advertisedHostTemplate;
    private Integer nodePort;
    private HttpGatewayConfig gateway;
    private HttpIngressConfig ingress;

    @Override
    public ExternalAccessType getType() { return type; }
    public void setType(ExternalAccessType type) { this.type = type; }

    @Override
    public String getAdvertisedHostTemplate() { return advertisedHostTemplate; }
    public void setAdvertisedHostTemplate(String advertisedHostTemplate) { this.advertisedHostTemplate = advertisedHostTemplate; }

    public Integer getNodePort() { return nodePort; }
    public void setNodePort(Integer nodePort) { this.nodePort = nodePort; }

    public HttpGatewayConfig getGateway() { return gateway; }
    public void setGateway(HttpGatewayConfig gateway) { this.gateway = gateway; }

    public HttpIngressConfig getIngress() { return ingress; }
    public void setIngress(HttpIngressConfig ingress) { this.ingress = ingress; }
}
