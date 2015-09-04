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

module ceres.slice;

import ceres;
import ceres.node;

/// A CeresSlice represents a data file where all metric data from the given
/// startTime is stored in a contiguous stream that is interpreted at a
/// resolution of timeStep.

/// Gaps in slices when writing to disk are padded with NaN values up to
/// MAX_SLICE_GAP as set by the database driver.
struct CeresSlice
{
  import std.bitmanip;
  import std.conv;
  import std.exception;
  import std.file;
  import std.path;
  import std.range;
  import std.stdio;
  import std.string;

  const CeresNode node;
  const long startTime;
  const long timeStep;
  const string fsPath;

  /// Params:
  ///     node = The CeresNode this slice is associated with
  ///     startTime = The timestamp of the first value contained in this slice
  ///     timeStep = The seconds-per-point resolution of this slice
  /// Note:
  ///     This class generally should be instantiated through use of CeresNode.
  ///     See CeresNode.write and CeresNode.slices
  this(CeresNode node, long startTime, long timeStep)
  {
    this.node = node;
    this.startTime = startTime;
    this.timeStep = timeStep;
    this.fsPath = buildPath(node.fsPath, "%s@%s.slice".format(startTime, timeStep));
  }

  /// Create a new CeresSlice on disk with the specified time and resolution.
  /// Params:
  ///     node = The CeresNode this slice is associated with
  ///     startTime = The timestamp of the first value contained in this slice
  ///     timeStep = The seconds-per-point resolution of this slice
  /// Returns:
  ///     A CeresSlice
  static CeresSlice create(CeresNode node, long startTime, long timeStep)
  {
    CeresSlice slice = CeresSlice(node, startTime, timeStep);
    File fileHandle = File(slice.fsPath, "wb");
    fileHandle.close();
    slice.fsPath.setAttributes(octal!644);
    return slice;
  }

  /// Writes the given datapoint to disk at a location calculated from the timestamp.
  /// Params:
  ///     datapoint = A Datapoint entry "(timestamp, value)"
  void write(Datapoint datapoint)
  {
    long timeOffset = datapoint.timestamp - this.startTime;
    long pointOffset = timeOffset / this.timeStep;
    long byteOffset = pointOffset * datapointSize;

    if (!this.fsPath.exists)
      throw new SliceDeleted(this.fsPath);

    File fileHandle = File(this.fsPath, "r+b");
    fileHandle.lock();
    long filesize = fileHandle.size;

    // Pad the allowable gap with nan's
    long byteGap = byteOffset - filesize;
    ubyte[] packedGap = [];
    if (byteGap > 0)
    {
      long pointGap = byteGap / datapointSize;
      if (pointGap > maxSliceGap)
        throw new SliceGapTooLarge();
      else
      {
        ubyte[] packedNaN = nativeToBigEndian(double.nan);
        packedGap = repeat(packedNaN).take(pointGap).array().join();
        byteOffset -= byteGap;
      }
    }

    try fileHandle.seek(byteOffset);
    catch (ErrnoException e)
      throw new Exception("%s: fsPath=%s byteOffset=%s size=%s value=%s"
          .format(e.msg, this.fsPath, byteOffset, filesize, datapoint.value));

    if (packedGap.length)
      fileHandle.rawWrite(packedGap);

    ubyte[datapointSize] packedValue = nativeToBigEndian(datapoint.value);
    fileHandle.rawWrite(packedValue);

    fileHandle.unlock();
    fileHandle.close();
  }
}

// vim: set sw=2 sts=2 tw=120 et cin :
