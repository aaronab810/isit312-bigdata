# HDFS ingestion evidence

Course VM: BigDataVM-2021v2_2. Hadoop version reported by `hadoop version`: 2.7.3. HDFS root: `/isit312/vet`. Source: TailSignal dataset and selected 14-file manifest. Results below were transcribed from the VM terminal output supplied during the run; attach the original screenshots and full logs separately.

## Clean upload and recovery from compatibility error

The first attempt printed `UPLOADED_AND_VERIFIED` for all 14 files, then stopped before publication because Hadoop 2.7 rejected `find -type`. The loader was corrected to use `hadoop fs -ls -R`. Retrying the same batch completed:

```text
BATCH_ACCEPTED batch=tailsignal_2026_11_22 path=/isit312/vet/raw/tailsignal_2026_11_22
```

## Identical replay

The user reported that an identical rerun printed `REPLAY_ALREADY_ACCEPTED`. Capture the complete original terminal output or screenshot as supporting evidence.

## Interrupted upload and retry

A separate `tailsignal_interrupt_test` batch was interrupted with Ctrl+C after several verified uploads. `hadoop fs -test -e /isit312/vet/raw/tailsignal_interrupt_test; echo $?` returned `1` before retry. The user reported that the retry displayed `RESUME_VERIFIED`, then `UPLOADED_AND_VERIFIED`, and ended:

```text
BATCH_ACCEPTED batch=tailsignal_interrupt_test path=/isit312/vet/raw/tailsignal_interrupt_test
```

The observed interruption occurred after several completed uploads. Whether it occurred during an individual file copy was not established; the simulated test covers a partial temporary file but is not evidence of a real interrupted in-flight HDFS copy.

## Changed source file

A separate temporary dataset copy was modified and used with the existing accepted batch ID. Actual output:

```text
LOCAL_MANIFEST_MISMATCH DATA_GUIDE.md expected=2070/9d31060b9433718054062a14e594ebf77f52fa8a77d6d42aed330a171b652589 actual=2088/af6a96c3eb403b7aec5e84de2e21a9746000bd8dca872dacced9ac5adfa67b8c
```

The accepted HDFS original was checked using `hadoop fs -cat /isit312/vet/raw/tailsignal_2026_11_22/DATA_GUIDE.md | sha256sum`. Actual output matches the original manifest:

```text
9d31060b9433718054062a14e594ebf77f52fa8a77d6d42aed330a171b652589  -
```

## Remaining evidence

Attach screenshots and exact complete command logs. Record the final Git commit ID. The simulated recovery test produced `MOCK_TEST_PASS`; it is not a substitute for HDFS evidence. Confirm single-writer operation and review the loader's documented limitations before integration.
