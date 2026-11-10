# Architecture

These pages describe simple-q 1.0.

The containers inside simple-q are **Client**, **App**, **Config**, the **worker supervisor**, and **Redis**. Client sends. App runs the handler and the dead-letter hook. Config is the only environment read. The worker supervisor forks children and watches memory. Redis holds the queue.

This is the context diagram for that model.

```mermaid
C4Context
  title System Context
  System_Ext(cgroup, "cgroup", "Container memory.current, memory.stat, and memory.max.")
  System_Ext(exporter, "Python exporter", "OpenTelemetry exporter supplied by the application.")
  Person(producer, "Application", "Sends a task on the default queue and reads a worker heartbeat.")
  System(simple_q, "simple-q", "Queue runtime for Python async tasks.")
  Rel(producer, simple_q, "Sends tasks on the default queue")
  Rel(simple_q, cgroup, "Reads memory.current, inactive_fi...")
  Rel(simple_q, exporter, "Exports finished spans on one OS ...")
  UpdateRelStyle(producer, simple_q, $textColor="#475569", $lineColor="#94a3b8", $offsetX="-30", $offsetY="-40")
  UpdateRelStyle(simple_q, cgroup, $textColor="#475569", $lineColor="#94a3b8", $offsetX="-8", $offsetY="-18")
  UpdateRelStyle(simple_q, exporter, $textColor="#475569", $lineColor="#94a3b8", $offsetX="14", $offsetY="4")
  UpdateLayoutConfig($c4ShapeInRow="2", $c4BoundaryInRow="1")
```
