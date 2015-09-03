// Copyright 2015 Iain Buclaw
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

module ceres.tree;

import ceres;
import ceres.metadata;
import ceres.node;

/// Represents a tree of Ceres metrics contained within a single path on disk
/// This is the primary Ceres API.
struct CeresTree
{
  import std.file;
  import std.path;
  import std.string;

  string root;
  CeresNode[string] nodeCache;

  /// Params:
  ///     root = The directory root of the Ceres tree
  this(string root)
  {
    if (!root.exists || !root.isDir)
      throw new Error("invalid root directory '%s'".format(root));
    this.root = absolutePath(root);
    this.nodeCache = null;
  }

  /// Get the on-disk path of a Ceres node given a metric name
  /// Params:
  ///     nodePath = A metric name e.g. "carbon.agents.graphite-a.cpuUsage"
  /// Returns:
  ///     The Ceres node path on disk
  string getFilesystemPath(string nodePath)
  {
    return buildPath(this.root, nodePath.replace(".", dirSeparator));
  }

  /// Returns whether the Ceres tree contains the given metric
  /// Params:
  ///     nodePath = A metric name e.g. "carbon.agents.graphite-a.cpuUsage"
  /// Returns:
  ///     true or false
  bool hasNode(string nodePath)
  {
    string fsPath = this.getFilesystemPath(nodePath);
    return fsPath.exists && fsPath.isDir;
  }

  /// Returns a Ceres node given a metric name
  /// Params:
  ///     nodePath = A metric name
  /// Returns:
  ///     A CeresNode
  CeresNode getNode(string nodePath)
  {
    CeresNode *node = (nodePath in this.nodeCache);
    if (node is null)
    {
      string fsPath = this.getFilesystemPath(nodePath);
      if (CeresNode.isNodeDir(fsPath))
      {
        CeresNode newnode = CeresNode(this, nodePath, fsPath);
        this.nodeCache[nodePath] = newnode;
        return newnode;
      }
      throw new NodeNotFound("the node '%s' does not exist in this tree".format(nodePath));
    }
    return *node;
  }

  /// Creates a new metric given a new metric name and optional per-node metadata
  /// Params:
  ///     nodePath = The new metric name
  ///     properties = Arbitrary key-value properties to store as metric metadata
  /// Returns:
  ///     A CeresNode
  CeresNode createNode(string nodePath, CeresMetadata properties)
  {
    return CeresNode.create(this, nodePath, properties);
  }

  /// Store a list of datapoints associated with a metric
  /// Params:
  ///     nodePath = The metric name to write to e.g. "carbon.agents.graphite-a.cpuUsage"
  ///     datapoint =  A Datapoint entry: "(timestamp, value)"
  void store(string nodePath, Datapoint datapoint)
  {
    CeresNode node = this.getNode(nodePath);
    node.write(datapoint);
  }
}

// vim: set sw=2 sts=2 tw=120 et cin :
