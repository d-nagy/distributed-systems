# Distributed Systems Assignment 2018/19

## Deadline

**16:00 on Friday 8th March 2019**

---

## Instructions

Use RMI in Java/Python to implement a reliable distributed replication system
based on the gossip replication architecture, to retrieve, update and submit
movie ratings.

## Requirements

- Draw a simple diagram to depict the system you have implemented. It should
  clearly illustrate major operation workflow flows between servers, a client
  and the front-end.

- Construct replication servers to maintain movie rating information. At least
  3 servers should be implemented. Simulate server availability and failure
  situations by having them arbitrarily report themselves as "active", "over-loaded"
  or "offline".

- Construct a front-end server. It serves as the entry point for a client to access
  the distributed system. It should react to server availability.

- Construct a client program. It provides an interface for the user to retrieve,
  submit and update movie ratings. This is just a text-based UI.

- Apply a suitable consistency control mechanism to support the gossip replication
  architecture. Include a suitable data message design to support such a mechanism.

## Mark scheme

| Section               | Marks |
| --------------------- | ----- |
| Diagram               | 10    |
| Replication servers   | 30    |
| Front-end server      | 25    |
| Client program        | 15    |
| Consistency control   | 20    |

## Submission

Single zip file containing all source code and diagram, including a readme with
instructions for running your program and a 200 word description to highlight the
main application features you have provided.