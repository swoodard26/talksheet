name := "xlsx-agent-system"

version := "0.1.0"

scalaVersion := "2.13.12"

// Adjust if you want a newer Akka, but 2.8.x is the stable release series.
val akkaVersion = "2.8.5"
val akkaHttpVersion = "10.5.3"

libraryDependencies ++= Seq(
  // --- Akka Typed Core ---
  "com.typesafe.akka" %% "akka-actor-typed"   % akkaVersion,
  "com.typesafe.akka" %% "akka-stream"        % akkaVersion,
  "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,

  // --- Akka HTTP ---
  "com.typesafe.akka" %% "akka-http"          % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion, // optional JSON support

  // --- For file upload streaming support ---
  "com.typesafe.akka" %% "akka-stream"        % akkaVersion,

  // --- Apache POI for .xlsx parsing ---
  "org.apache.poi"    % "poi-ooxml"           % "5.2.5",

  // --- SQLite in-memory execution ---
  "org.xerial"       % "sqlite-jdbc"         % "3.45.2.0",

  // --- Logging (SLF4J + Logback) ---
  "ch.qos.logback"    % "logback-classic"     % "1.4.12",

  // --- Test dependencies ---
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "org.scalatest"     %% "scalatest"                % "3.2.18"    % Test
)

// Needed for fewer Akka warnings
Compile / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-encoding", "utf8"
)

Compile / mainClass := Some("talksheet.ai.app.Main")
