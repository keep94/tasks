// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

package tasks

import (
  "errors"
  "github.com/keep94/tasks/recurring"
  "testing"
  "time"
)

var (
  kNow = time.Date(2013, 9, 12, 17, 21, 0, 0, time.Local)
  kSomeError = errors.New("some error")
)

func TestParallel(t *testing.T) {
  testTasks := make([]Task, 20)
  for i := range testTasks {
    testTasks[i] = &hasRunTask{}
  }
  e := Start(ParallelTasks(testTasks...))
  <-e.Done()

  // Blocking here is not necessary in production code. Just testing that
  // this channel gets closed too.
  <-e.Ended()
  for _, atask := range testTasks {
    bt := atask.(*hasRunTask)
    if !bt.hasRun {
      t.Error("Expected task to be run.")
    }
  }
}

func TestSeries(t *testing.T) {
  testTasks := make([]Task, 20)
  for i := range testTasks {
    testTasks[i] = &hasRunTask{}
  }
  e := Start(SerialTasks(testTasks...))
  <-e.Done()

  // Blocking here is not necessary in production code. Just testing that
  // this channel gets closed too.
  <-e.Ended()
  for _, atask := range testTasks {
    bt := atask.(*hasRunTask)
    if !bt.hasRun {
      t.Error("Expected task to be run.")
    }
  }
}

func TestEndTask(t *testing.T) {
  longTask := &longRunningTask{}
  e := Start(longTask)
  e.End()
  <-e.Done()
  if !longTask.hasRun {
    t.Error("Expected task to be run.")
  }
}

func TestNoError(t *testing.T) {
  eTask := &errorTask{}
  e := Start(eTask)
  <-e.Done()
  if e.Error() != nil {
    t.Error("Expected no error.")
  }
}

func TestNoError2(t *testing.T) {
  eTask := &errorTask{}
  if err := Run(eTask); err != nil {
    t.Error("Expected no error.")
  }
}

func TestError(t *testing.T) {
  eTask := &errorTask{kSomeError}
  e := Start(eTask)
  <-e.Done()
  if e.Error() != kSomeError {
    t.Error("Expected some error.")
  }
}

func TestError2(t *testing.T) {
  eTask := &errorTask{kSomeError}
  if err := Run(eTask); err != kSomeError {
    t.Error("Expected some error.")
  }
}

func TestRecurring(t *testing.T) {
  timeTask := &timeStampTask{}
  r := recurring.FirstN(
      recurring.AtInterval(time.Hour),
      2)
  RunForTesting(RecurringTask(timeTask, r), &ClockForTesting{kNow})
  verifyTimes(t, timeTask.timeStamps, kNow.Add(time.Hour), kNow.Add(2 * time.Hour))
}

type hasRunTask struct {
  hasRun bool
}

func (bt *hasRunTask) Do(e *Execution) error {
  bt.hasRun = true
  return nil
}

type longRunningTask struct {
  hasRun bool
}

func (lt *longRunningTask) Do(e *Execution) error {
  e.Sleep(time.Hour)
  lt.hasRun = true
  return nil
}

type errorTask struct {
  err error
}

func (et *errorTask) Do(e *Execution) error {
  return et.err
}

type timeStampTask struct {
  timeStamps []time.Time
}

func (tt *timeStampTask) Do(e *Execution) error {
  tt.timeStamps = append(tt.timeStamps, e.Now())
  return nil
}

func verifyTimes(t *testing.T, actual []time.Time, expected ...time.Time) {
  if len(actual) != len(expected) {
    t.Errorf("Expected %v timestamps, got %v", len(expected), len(actual))
  }
  for i := range expected {
    if expected[i] != actual[i] {
      t.Errorf("Expected time %v at %d, got %v", expected[i], i, actual[i])
    }
  }
}
