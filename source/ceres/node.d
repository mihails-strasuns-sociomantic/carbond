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

module ceres.node;

import ceres;
import ceres.metadata;
import ceres.slice;
import ceres.tree;

/// A CeresNode represents a single time-series metric of a given timeStep
/// (its seconds-per-point resolution) and containing arbitrary key-value metadata.

/// A CeresNode is associated with its most precise timeStep. This timeStep is the finest
/// resolution that can be used for writing, though a CeresNode can contain and read data with
/// other, less-precise timeStep values in its underlying CeresSlice data.
struct CeresNode
{
  import std.algorithm;
  import std.array;
  import std.conv;
  import std.file;
  import std.math;
  import std.path;
  import std.range;
  import std.string;

  CeresTree tree;
  string nodePath;
  string fsPath;
  string metadataFile;
  long timeStep;

  /// Params:
  ///     tree = The CeresTree this node is associated with
  ///     nodePath = The name of the metric this node represents
  ///     fsPath = The filesystem path of this metric
  /// Note:
  ///     This class generally should be instantiated through use of CeresTree.
  ///     See CeresTree.createNode and CeresTree.getNode
  this(CeresTree tree, string nodePath, string fsPath)
  {
    this.tree = tree;
    this.nodePath = nodePath;
    this.fsPath = fsPath;
    this.metadataFile = buildPath(fsPath, ".ceres-node");
    this.timeStep = -1;
  }

  /// Create a new CeresNode on disk with the specified properties.
  /// Params:
  ///     tree = The CeresTree this node is associated with
  ///     nodePath = The name of the metric this node represents
  ///     properties = A set of key-value properties to be associated with this node
  /// Note:
  ///     A CeresNode always has the timeStep property which is an integer value
  ///     representing the precision of the node in seconds-per-datapoint.
  ///     E.g. a value of 60 represents one datapoint per minute.  If no timeStep
  ///     is specified at creation, the value of ceres.node.defaultTimeStep is used.
  /// Returns:
  ///     A CeresNode
  static CeresNode create(CeresTree tree, string nodePath, CeresMetadata properties)
  {
    // Create the node directory
    string fsPath = tree.getFilesystemPath(nodePath);
    if (!fsPath.exists)
      mkdirRecurse(fsPath);

    // Create the initial metadata
    CeresNode node = CeresNode(tree, nodePath, fsPath);
    properties["timeStep"] = properties.get("timeStep", defaultTimeStep);
    node.writeMetadata(properties);

    return node;
  }

  /// Tests whether the given path is a CeresNode
  /// Params:
  ///     path = Path to test
  /// Returns
  ///     true or false
  static bool isNodeDir(string path)
  {
    return path.exists && path.isDir
      && buildPath(path, ".ceres-node").exists;
  }

  /// Update node metadata from disk
  /// Throws:
  ///     A CorruptNode
  CeresMetadata readMetadata()
  {
    try
    {
      string data = cast(string) std.file.read(this.metadataFile);
      CeresMetadata metadata = jsonLoad(data);
      this.timeStep = metadata["timeStep"].coerce!(long);
      return metadata;
    }
    catch(Exception e)
      throw new CorruptNode("unable to parse node metadata: %s".format(e.msg));
  }

  /// Writes new metadata to disk
  /// Params:
  ///     metadata = a JSON-serializable CeresMetadata of node metadata
  void writeMetadata(CeresMetadata metadata)
  {
    this.timeStep = metadata["timeStep"].coerce!(long);
    std.file.write(this.metadataFile, jsonDump(metadata));
  }

  /// A property providing access to information about this node's underlying slices.
  /// Returns:
  ///     [(startTime, timeStep), ...]
  CeresSlice[] slices() @property
  {
    CeresSlice[] ceresslices = [];

    foreach (slice; this.readSlices())
      ceresslices ~= CeresSlice(this, slice.tupleof);

    return ceresslices;
  }

  /// Read slice information from disk
  /// Returns:
  ///     [(startTime, timeStep), ...]
  SliceData[] readSlices()
  {
    if (!this.fsPath.exists)
      throw new NodeDeleted("node deleted: %s".format(this.fsPath));

    SliceData[] slice_info = [];

    auto entries = dirEntries(this.fsPath, SpanMode.shallow)
      .filter!(a => endsWith(a.name, ".slice"))
      .map!(a => baseName(a.name))
      .array;

    foreach (filename; entries)
    {
      string[] parts = filename[0 .. $-6].split("@");
      long startTime = to!long(parts[0]);
      long timeStep = to!long(parts[1]);
      slice_info ~= SliceData(startTime, timeStep);
    }
    return retro(sort(slice_info)).array();
  }

  /// Writes the given datapoint to underlying slices.
  /// Params:
  ///     datapoint = A Datapoint entry "(timestamp, value)"
  void write(Datapoint datapoint)
  {
    if (this.timeStep == -1)
      this.readMetadata();

    if (isNaN(datapoint.value))
      return;

    bool dataWritten = false;

    foreach (slice; this.slices)
    {
      if (slice.timeStep != this.timeStep)
        continue;

      if (datapoint.timestamp >= slice.startTime)
      {
        try slice.write(datapoint);
        catch (SliceGapTooLarge)
        {
          auto newSlice = CeresSlice.create(this, datapoint.timestamp, slice.timeStep);
          newSlice.write(datapoint);
        }
        catch (SliceDeleted)
        {
          // Recurse to retry
          this.write(datapoint);
          return;
        }
        dataWritten = true;
        break;
      }
    }

    if (!dataWritten)
    {
      auto slice = CeresSlice.create(this, datapoint.timestamp, this.timeStep);
      slice.write(datapoint);
    }
  }
}

// vim: set sw=2 sts=2 tw=120 et cin :
