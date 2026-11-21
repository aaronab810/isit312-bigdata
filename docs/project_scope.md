# Project scope

The ISIT312 project studies the existing data workflow of a real UAE veterinary clinic, implements a data lake that addresses a problem in that workflow and evaluates the solution with measurements and company feedback.

## What the brief requires

- **Clinic study and motivation.** Study the real UAE clinic’s data volume, collection, storage, processing, integration and recurring issues. Use clinic-provided facts to explain the problem and justify the design. TailSignal supplies implementation and test data.
- **Architecture and implementation.** Build a repeatable data lake on the course VM. Land exports in HDFS, normalize and integrate with Scala Spark SQL, write Parquet, and demonstrate working Hive/Beeline jobs. Show how source IDs, exceptions and child records stay traceable.
- **Evaluation and trade-offs.** Measure data quality and report reconciliation. Compare Spark SQL and MapReduce using the same data and workload at two sizes, then explain which approach fits the clinic’s requirements and where each falls short. Use replay tests for late and corrected files.
- **Company evaluation and submission.** Record the clinic’s assessment, resulting design changes and lessons learned. Submit the report, codebase, architecture diagram, screenshots and sample job evidence, and a presentation that follows the report.

## Role of the live demo

The live demo shows monthly visit counts and one visit traced back to its original note and lab results. It provides evidence for the architecture, the integration rules and the reporting logic.

The passing HDFS and Spark baseline gives everyone local inputs. The completed team project also needs the merged run through Hive/Beeline, the empirical comparison and the clinic’s assessment.

## Report and presentation

Executive summary → motivation → technical requirements → main constructs → implementation with architecture, pipelines, running jobs, screenshots and code → limitations and constraints → conclusion. Use the same sequence for the shorter presentation.

See the [parallel work packages](work_packages.md) for ownership, deliverables and starting inputs.
