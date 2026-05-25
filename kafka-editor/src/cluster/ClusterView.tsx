import { useState } from 'react'
import { ClusterNav } from './ClusterNav'
import { ClusterOverview } from './overview/ClusterOverview'
import { TopicsList } from './topics/TopicsList'
import { TopicDetail } from './topics/TopicDetail'
import { MessageBrowser } from './messages/MessageBrowser'
import { ConsumerGroupsList } from './groups/ConsumerGroupsList'
import { GroupDetail } from './groups/GroupDetail'
import { SchemaRegistryView } from './schemas/SchemaRegistryView'
import { AclsView } from './acls/AclsView'
import { ConnectorsView } from './connect/ConnectorsView'
import { ClusterManagerModal } from './ClusterManagerModal'
import { useActiveCluster } from './clusterStore'
import { useViewStore } from '../state/viewStore'
import { EmptyState } from '../components/EmptyState'

/**
 * The Cluster (management) view: left nav + the active section. The section and
 * selected topic live in viewStore, so cross-links navigate it with one write.
 * Sections beyond Overview and Topics arrive in later milestones.
 */
export function ClusterView() {
  const section = useViewStore((s) => s.clusterSection)
  const selectedTopic = useViewStore((s) => s.selectedTopic)
  const setClusterSection = useViewStore((s) => s.setClusterSection)
  const setSelectedTopic = useViewStore((s) => s.setSelectedTopic)
  const [showManager, setShowManager] = useState(false)
  const [browsingMessages, setBrowsingMessages] = useState(false)
  const [selectedGroup, setSelectedGroup] = useState<string | null>(null)
  const active = useActiveCluster()

  if (!active) {
    return (
      <div className="flex flex-1 flex-col">
        <EmptyState
          title="No cluster selected"
          hint="Add a Kafka cluster connection to browse topics, messages and consumer groups."
          action={
            <button
              type="button"
              onClick={() => setShowManager(true)}
              className="rounded border border-emerald-700 bg-emerald-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-emerald-700"
            >
              Add a cluster
            </button>
          }
        />
        {showManager && (
          <ClusterManagerModal onClose={() => setShowManager(false)} />
        )}
      </div>
    )
  }

  return (
    <div className="flex flex-1 overflow-hidden">
      <ClusterNav
        section={section}
        onSelect={(s) => {
          setBrowsingMessages(false)
          setSelectedGroup(null)
          setClusterSection(s)
        }}
      />
      <div className="flex flex-1 flex-col overflow-hidden">
        {section === 'overview' && <ClusterOverview cluster={active} />}
        {section === 'topics' &&
          (selectedTopic === null ? (
            <TopicsList
              cluster={active}
              onSelectTopic={(name) => {
                setBrowsingMessages(false)
                setSelectedTopic(name)
              }}
            />
          ) : browsingMessages ? (
            <MessageBrowser
              key={selectedTopic}
              cluster={active}
              topic={selectedTopic}
              onBack={() => setBrowsingMessages(false)}
            />
          ) : (
            <TopicDetail
              key={selectedTopic}
              cluster={active}
              topic={selectedTopic}
              onBack={() => setSelectedTopic(null)}
              onBrowseMessages={() => setBrowsingMessages(true)}
            />
          ))}
        {section === 'groups' &&
          (selectedGroup === null ? (
            <ConsumerGroupsList
              cluster={active}
              onSelectGroup={setSelectedGroup}
            />
          ) : (
            <GroupDetail
              key={selectedGroup}
              cluster={active}
              groupId={selectedGroup}
              onBack={() => setSelectedGroup(null)}
            />
          ))}
        {section === 'schemas' && <SchemaRegistryView cluster={active} />}
        {section === 'acls' && <AclsView cluster={active} />}
        {section === 'connect' && <ConnectorsView cluster={active} />}
      </div>
    </div>
  )
}
