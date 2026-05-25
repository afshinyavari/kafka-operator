package se.afshin.yavari.kafka.editor.interpreter;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import se.afshin.yavari.kafka.editor.model.ProjectEdge;
import se.afshin.yavari.kafka.editor.model.ProjectNode;

/** Topological sort of the topology graph (Kahn's algorithm). */
public final class TopoSort {

    private TopoSort() {
    }

    /** Returns the nodes in dependency order. Throws if the graph has a cycle. */
    public static List<ProjectNode> sort(List<ProjectNode> nodes, List<ProjectEdge> edges) {
        Map<String, Integer> indegree = new HashMap<>();
        Map<String, List<String>> adjacency = new HashMap<>();
        Map<String, ProjectNode> byId = new HashMap<>();
        for (ProjectNode node : nodes) {
            indegree.put(node.id(), 0);
            adjacency.put(node.id(), new ArrayList<>());
            byId.put(node.id(), node);
        }
        for (ProjectEdge edge : edges) {
            if (!indegree.containsKey(edge.source()) || !indegree.containsKey(edge.target())) {
                continue;
            }
            adjacency.get(edge.source()).add(edge.target());
            indegree.merge(edge.target(), 1, Integer::sum);
        }

        Deque<String> queue = new ArrayDeque<>();
        for (ProjectNode node : nodes) {
            if (indegree.get(node.id()) == 0) {
                queue.add(node.id());
            }
        }
        List<ProjectNode> order = new ArrayList<>();
        while (!queue.isEmpty()) {
            String id = queue.poll();
            order.add(byId.get(id));
            for (String next : adjacency.get(id)) {
                if (indegree.merge(next, -1, Integer::sum) == 0) {
                    queue.add(next);
                }
            }
        }
        if (order.size() != nodes.size()) {
            throw new IllegalArgumentException("The topology contains a cycle");
        }
        return order;
    }
}
