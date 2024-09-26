package feign.ranger.selector;

import com.flipkart.ranger.finder.sharded.MapBasedServiceRegistry;
import com.flipkart.ranger.model.ServiceNode;
import com.flipkart.ranger.model.ShardSelector;
import com.google.common.collect.ListMultimap;

import feign.ranger.common.ShardInfo;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class DynamicEnvironmentAwareShardSelector implements ShardSelector<ShardInfo, MapBasedServiceRegistry<ShardInfo>> {

    private static final String ALL_ENV = "*";

    @Override
    public List<ServiceNode<ShardInfo>> nodes(final ShardInfo criteria,
                                              final MapBasedServiceRegistry<ShardInfo> serviceRegistry) {
        val serviceNodes = serviceRegistry.nodes();
        val serviceName = serviceRegistry.getService().getServiceName();
        val environment = criteria.getEnvironment();

        if (Objects.equals(environment, ALL_ENV)) {
            return allNodes(serviceNodes);
        }
        final Collection<ServiceNode<ShardInfo>> currentEnvNodes = serviceNodes.asMap()
                .get(criteria);
        if (currentEnvNodes != null && !currentEnvNodes.isEmpty()) {
            log.debug("Effective environment for discovery of {} is {}", serviceName, environment);
            return new ArrayList<>(currentEnvNodes);
        }
        return Collections.emptyList();
    }

    private List<ServiceNode<ShardInfo>> allNodes(ListMultimap<ShardInfo, ServiceNode<ShardInfo>> serviceNodes) {
        return serviceNodes.asMap()
                .values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }
}
