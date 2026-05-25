/**
 * Registers every node type. Each imported module calls `registerNode(...)` on
 * load. Later milestones add stateful ops (M4) and Kafka Connect connectors
 * (M5) by adding a file here.
 */
import './sources'
import './conversions'
import './stateless'
import './stateful'
import './joins'
import './sinks'
import './connectors'
