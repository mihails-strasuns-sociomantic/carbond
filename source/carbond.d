// Written in the D Programming Language.
module carbond;

import ceres;

import vibe.core.args;
import vibe.core.core;
import vibe.core.log;
import vibe.core.net;
import vibe.core.stream;
import vibe.stream.operations;

import core.time;
import core.stdc.stdlib;
import core.sys.posix.syslog;
import std.conv;
import std.file;
import std.format;
import std.math;
import std.ini;
import std.path;
import std.process;
import std.regex;
import std.stdio;
import std.string;

/// Default storage rules based off carbon.conf.py
struct StorageRule
{
  bool match_all = false;
  Regex!(char) pattern;
  long[][] retentions = [[60, 10080]];    // Minutely data for a week
  string xfilesfactor = "0.5";
  string aggregation_method = "average";
}

/// A log observer for logging to syslog based off carbon.log.py
final class SyslogObserver : Logger
{
  immutable string prefix;

  this(string prefix)
  {
    this.prefix = prefix.idup;
    this.minLevel = LogLevel.warn;
  }

  override void log(ref LogLine msg) @trusted
  {
    static bool log_open = false;
    if (!log_open)
    {
      openlog(toStringz(this.prefix), 0, LOG_USER);
      log_open = true;
    }

    int priority = 0;
    final switch(msg.level)
    {
      case LogLevel.none:       assert(false);
      case LogLevel.trace:      return;
      case LogLevel.debugV:     return;
      case LogLevel.debug_:     priority = LOG_DEBUG; break;
      case LogLevel.diagnostic: priority = LOG_INFO; break;
      case LogLevel.info:       priority = LOG_NOTICE; break;
      case LogLevel.warn:       priority = LOG_WARNING; break;
      case LogLevel.error:      priority = LOG_ERR; break;
      case LogLevel.critical:   priority = LOG_CRIT; break;
      case LogLevel.fatal:      priority = LOG_ALERT; break;
    }

    syslog(priority, toStringz(msg.text));
  }
}

/// Globals
__gshared CeresTree database;
__gshared StorageRule[] storage_rules;
shared Logger logger;

///
void setupDatabase(string config_path)
{
  string dbname;
  string datadir;
  uint slicegap;

  string config = buildPath(config_path, "db.conf");
  IniFile db = open(config);
  try dbname = db["DATABASE"];
  catch (Exception next)
    throw new Error("(" ~ config ~ "): missing setting for 'DATABASE'", next);
  try datadir = db["LOCAL_DATA_DIR"];
  catch (Exception next)
    throw new Error("(" ~ config ~ "): missing setting for 'LOCAL_DATA_DIR'", next);

  auto dbconfig = db.get(dbname);
  if (!dbconfig)
    throw new Error("(" ~ config ~ "): missing section for [" ~ dbname ~ "]");
  else
  {
    string max_slice_gap;
    try max_slice_gap = dbconfig["MAX_SLICE_GAP"];
    catch (Exception next)
      throw new Error("(" ~ config ~ "): missing setting for 'MAX_SLICE_GAP' in [" ~ dbname ~ "]", next);
    try slicegap = to!uint(max_slice_gap);
    catch (Exception next)
      throw new Error("(" ~ config ~ "): invalid MAX_SLICE_GAP '" ~ max_slice_gap ~ "' in [" ~ dbname ~ "]", next);
  }

  ceres.maxSliceGap = slicegap;
  database = CeresTree(datadir);
}

///
void setupStorageRules(string config_path)
{
  long[char] UnitMultipliers = [
    's': 1,
    'm': 60,
    'h': 60 * 60,
    'd': 60 * 60 * 24,
    'w': 60 * 60 * 24 *7,
    'y': 60 * 60 * 24 *365
  ];

  string config = buildPath(config_path, "storage-rules.conf");
  IniFile rules = open(config);
  foreach (rule; rules)
  {
    StorageRule storage_rule;
    foreach (key; rule.keys)
    {
      if (key == "match-all")
        storage_rule.match_all = to!bool(rule["match-all"]);
      else if (key == "pattern")
        storage_rule.pattern = regex(rule["pattern"]);
      else if (key == "retentions")
      {
        long[][] retentions;
        foreach (retention; rule["retentions"].split(','))
        {
          string[] parts = retention.strip.split(":");
          if (parts.length != 2)
            throw new Error("(" ~ config ~ "): invalid retention '" ~ retention ~ "' in [" ~ rule.name ~ "]");
          long precision;
          long points;
          char precisionUnit;
          char pointsUnit;
          try
          {
            try
            {
              precision = to!long(parts[0]);
              precisionUnit = 's';
            }
            catch (Exception)
            {
              precision = to!long(parts[0][0 .. $-1]);
              precisionUnit = parts[0][$-1];
            }
            try
            {
              points = to!long(parts[1]);
              pointsUnit = 's';
            }
            catch (Exception)
            {
              points = to!long(parts[1][0 .. $-1]);
              pointsUnit = parts[1][$-1];
            }
          }
          catch (Exception next)
            throw new Error("(" ~ config ~ "): invalid retention '" ~ retention ~ "' in [" ~ rule.name ~ "]", next);

          if (precisionUnit !in UnitMultipliers)
            throw new Error("(" ~ config ~ "): invalid unit '" ~ precisionUnit ~ "' in [" ~ rule.name ~ "]");

          if (pointsUnit !in UnitMultipliers)
            throw new Error("(" ~ config ~ "): invalid unit '" ~ pointsUnit ~ "' in [" ~ rule.name  ~ "]");

          precision = precision * UnitMultipliers[precisionUnit];

          if (pointsUnit != 's')
            points = points * UnitMultipliers[pointsUnit] / precision;

          retentions ~= [precision, points];
        }
        storage_rule.retentions = retentions;
      }
    }

    if (storage_rule.match_all && !storage_rule.pattern.empty)
      throw new Error("(" ~ config ~ "): Exactly one condition key must be provided per rule: match-all | pattern");

    storage_rules ~= storage_rule;
  }
}

///
void setupServer(string config_path)
{
  string address;
  ushort port;
  string config = buildPath(config_path, "listeners.conf");
  IniFile listener = open(config);
  if (!listener.empty)
  {
    /// Read only the first entry.
    try address = listener.front["interface"];
    catch (Exception next)
      throw new Error("(" ~ config ~ "): missing setting for 'interface'", next);

    try port = to!ushort(listener.front["port"]);
    catch (Exception next)
      throw new Error("(" ~ config ~ "): missing setting for 'port'", next);
  }
  else
    throw new Error("(" ~ config ~ "): no defined listener interfaces");

  listenTCP(port, (conn) => metricReceiver(conn), address, TCPListenOptions.disableAutoClose);
}

/// Our network entry point.
void metricReceiver(TCPConnection conn)
{
  string metric;
  double value;
  ulong timestamp;

  while (conn.waitForData(5.seconds))
  {
    while (!conn.empty)
    {
      string line;

      try line = cast(string)conn.readLine(size_t.max, "\n");
      catch (Exception e)
      {
        logWarn(e.msg);
        break;
      }
      line.formattedRead("%s %s %s", &metric, &value, &timestamp);

      // Drop NaN and Inf data points since they are not supported values.
      if (isNaN(value) || isInfinity(value))
      {
        debug logInfo("dropping unsupported metric: %s", metric);
        continue;
      }

      debug logInfo("processing metric: %s", metric);

      if (!database.hasNode(metric))
      {
        // Determine metadata from storage rules
        CeresMetadata metadata;
        foreach (rule; storage_rules)
        {
          if (rule.match_all || (!rule.pattern.empty && !matchFirst(metric, rule.pattern).empty))
          {
            metadata["timeStep"] = rule.retentions[0][0];
            metadata["retentions"] = rule.retentions;
            metadata["xFilesFactor"] = rule.xfilesfactor;
            metadata["aggregationMethod"] = rule.aggregation_method;
            break;
          }
        }
        try database.createNode(metric, metadata);
        catch (Exception next)
        {
          logError("database create operation failed: %s", metric);
          continue;
        }
      }

      try database.store(metric, Datapoint(timestamp, value));
      catch (Exception next)
      {
        logError("database write operation failed: %s", metric);
        continue;
      }
    }
  }
  logWarn("closing connection stream");
  conn.close();
}

shared static this()
{
  try
  {
    try
    {
      /// Optional (compatibility) daemon options.
      bool nodaemon;
      bool syslog;
      string prefix = "carbond";
      readOption("n|nodaemon", &nodaemon, "Run in the foreground (noop)");
      readOption("s|syslog", &syslog, "Log to syslog, not to file");
      readOption("p|prefix", &prefix, "Use the given prefix when syslogging");

      /// Process all required options.
      string instance = readRequiredOption!string("i|instance", "Name of the carbond instance");

      /// Daemon configuration and environment.
      string graphite_root = environment.get("GRAPHITE_ROOT", dirName(absolutePath(".")));
      string storage_dir = environment.get("GRAPHITE_STORAGE_DIR", buildPath(graphite_root, "storage"));
      string conf_dir = environment.get("GRAPHITE_CONF_DIR", buildPath(graphite_root, "conf"));
      string config_path = buildPath(conf_dir, "carbon-daemons", instance);

      if (instance.length != 0)
      {
        /// Logging
        if (syslog)
          logger = cast(shared) new SyslogObserver(prefix);
        else
        {
          string log_dir = buildPath(storage_dir, "log");
          if (!exists(log_dir))
            mkdirRecurse(log_dir);
          logger = cast(shared) new FileLogger(buildPath(log_dir, "console.log"));
        }
        registerLogger(logger);

        /// Database
        setupDatabase(config_path);

        /// Storage Rules
        setupStorageRules(config_path);

        /// Server
        setupServer(config_path);

        /// Default log level (overriden by verbose options)
        setLogLevel(LogLevel.warn);
      }
    }
    catch (Exception next)
      throw new Error(next.msg, next.next);
  }
  catch (Error error)
  {
    // Log cascading errors and quit on failure.
    for (Throwable thrown = error.next; thrown !is null; thrown = thrown.next)
      logError(thrown.msg);

    logFatal(error.msg);
    exit(EXIT_FAILURE);
  }
}

// vim: set sw=2 sts=2 tw=120 et cin :
