import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.util.StopWatch;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.wiremock.integrations.testcontainers.WireMockContainer;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

import java.time.Duration;

@Testcontainers
@Slf4j
public class WebClientTimeoutsIT {

    private static final Network network = Network.newNetwork();

    @Container
    private static final WireMockContainer WIREMOCK = new WireMockContainer("wiremock/wiremock:3.6.0")
            .withExposedPorts(8080)
            .withMappingFromResource(
                    "mocks-config.json")
            .withNetwork(network).withNetworkAliases("wiremock");

    @Container
    private static final ToxiproxyContainer TOXI_PROXY = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0")).dependsOn(WIREMOCK).withNetwork(network);

    @BeforeAll
    @SneakyThrows
    static void setUp() {
        getToxiproxyClient().createProxy("wiremock", "0.0.0.0:8666", "wiremock:" + "8080");
    }

    private static ToxiproxyClient getToxiproxyClient() {
        return new ToxiproxyClient(TOXI_PROXY.getHost(), TOXI_PROXY.getControlPort());
    }

    private String getProxiedUrl(String path) {
        var ipAddressViaToxiproxy = TOXI_PROXY.getHost();
        var portViaToxiproxy = TOXI_PROXY.getMappedPort(8666);
        var proxyUrl = "http://" + ipAddressViaToxiproxy + ":" + portViaToxiproxy;
        return proxyUrl + path;
    }

    @BeforeEach
    @SneakyThrows
    public void before() {
        getToxiproxyClient().reset();
    }

    @Test
    @SneakyThrows
    void serverStreamingResponse() {

        getToxiproxyClient().getProxy("wiremock").toxics()
                .slicer("chunked", ToxicDirection.DOWNSTREAM, 1, Duration.ofMillis(100).toMillis() * 1000);

        var httpClientSettings = HttpClientSettings.createDefault();
        httpClientSettings.setResponseTimeout(Duration.ofMillis(17000));

        var webClient = createCustom("test-client", httpClientSettings).build();
        var watch = new StopWatch();
        watch.start("response arrival");
        var result = webClient
                .get()
                .uri(getProxiedUrl("/albums/1/photos"))
                .exchangeToMono((clientResponse) -> {
                    watch.stop();
                    log.info("Got status: {}", clientResponse.statusCode());
                    log.info("Got headers: {}", clientResponse.headers());
                    watch.start("getting response body");
                    return clientResponse.bodyToMono(String.class);
                });

        StepVerifier.create(result)
                .consumeNextWith(body -> {
                    log.info("Got body: {}", body);
                }).verifyComplete();
        watch.stop();
        log.info(watch.prettyPrint());
    }

    private HttpClientSettings getFinalProperties(String name, HttpClientSettings properties) {
        var clientSettings = properties.toBuilder()
                .maxConnections(properties.getMaxConnectionsTotal())
                .writeTimeout(null)
                .readTimeout(properties.getResponseTimeout())
                .build();
        if (!clientSettings.equals(properties)) {
            log.warn("Http client configuration settings for {} has been overwritten", name);
        }
        return clientSettings;
    }

    public WebClient.Builder createCustom(String name, HttpClientSettings properties) {
        var finalProperties = getFinalProperties(name, properties);
        log.info("Creating http client, type:web-client name:{} config:{}", name, finalProperties);
        var connectionProvider = ConnectionProvider.builder(name)
                .maxConnections(finalProperties.getMaxConnections())
                .maxLifeTime(finalProperties.getMaxConnectionLifeTime())
                .pendingAcquireMaxCount(-1) // no limit
                .pendingAcquireTimeout(finalProperties.getAcquireTimeout())
                .metrics(finalProperties.isMetrics())
                .build();

        var client = HttpClient.create(connectionProvider)
                .compress(true)
                .wiretap(properties.isVerbose())
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Long.valueOf(finalProperties.getConnectionTimeout().toMillis()).intValue())
                .option(ChannelOption.SO_KEEPALIVE, true)
                .responseTimeout(finalProperties.getResponseTimeout());

        if (finalProperties.isInsecure()) {
            client = client.secure(t -> t.sslContext(createInsecureSslContext()));
        }

        var connector = new ReactorClientHttpConnector(client);
        var webClient = WebClient.builder()
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .clientConnector(connector)
                .filter((request, next) -> next.exchange(request)
                        .flatMap(response -> {
                            if (response.statusCode().is5xxServerError()) {
                                return response.createException()
                                        .flatMap(Mono::error);
                            }
                            return Mono.just(response);
                        })
                        .retryWhen(Retry.backoff(properties.getRetries(), properties.getRetryDelay())
                                .filter(throwable -> {
                                    if (throwable instanceof WebClientResponseException webClientResponseException) {
                                        return webClientResponseException.getStatusCode().is5xxServerError();
                                    }
                                    return throwable instanceof WebClientRequestException;
                                })
                                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> retrySignal.failure())));
        return webClient;
    }

    @SneakyThrows
    private static SslContext createInsecureSslContext() {
        return SslContextBuilder
                .forClient()
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .build();
    }

    @Builder(toBuilder = true)
    @Data
    public static class HttpClientSettings {

        @Builder.Default
        Duration connectionTimeout = Duration.ofMillis(1_000);

        @Builder.Default
        Duration readTimeout = Duration.ofMillis(2_000);

        @Builder.Default
        Duration writeTimeout = Duration.ofMillis(2_000);

        @Builder.Default
        Duration responseTimeout = Duration.ofMillis(5_000);

        @Builder.Default
        Duration acquireTimeout = Duration.ofMillis(1_000);

        @Builder.Default
        int maxConnections = 250;

        @Builder.Default
        int maxConnectionsTotal = 250;

        @Builder.Default
        Duration maxConnectionLifeTime = Duration.ofMinutes(15);

        @Builder.Default
        Duration connectionValidation = Duration.ofSeconds(30);

        @Builder.Default
        boolean insecure = false;

        @Builder.Default
        boolean metrics = false;

        @Builder.Default
        int retries = 0;

        @Builder.Default
        Duration retryDelay = Duration.ofMillis(1_000);

        String proxyHost;

        int proxyPort;

        boolean verbose;

        public static HttpClientSettings createDefault() {
            return HttpClientSettings.builder().build();
        }
    }
}
