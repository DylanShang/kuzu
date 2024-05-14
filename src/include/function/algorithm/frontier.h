#pragma once

#include <vector>
#include <unordered_map>
#include <algorithm>

#include "function/hash/hash_functions.h"

namespace kuzu {
namespace function {

class Frontier {
public:
    void addNode(common::nodeID_t nodeID, uint32_t multiplicity) {
        if (nodeIDToMultiplicity.contains(nodeID)) {
            nodeIDToMultiplicity.at(nodeID) += multiplicity;
            return ;
        }
        nodeIDToMultiplicity.insert({nodeID, multiplicity});
        nodeIDs.push_back(nodeID);
    }

    void sort() {
        std::sort(nodeIDs.begin(), nodeIDs.end());
    }

    void clear() {
        nodeIDs.clear();
        nodeIDToMultiplicity.clear();
    }

    const std::vector<common::nodeID_t>& getNodeIDs() const {
        return nodeIDs;
    }

    uint32_t getMultiplicity(common::nodeID_t nodeID) const {
        KU_ASSERT(nodeIDToMultiplicity.contains(nodeID));
        return nodeIDToMultiplicity.at(nodeID);
    }

private:
    std::vector<common::nodeID_t> nodeIDs;
    common::node_id_map_t<uint32_t> nodeIDToMultiplicity;
};

}
}
