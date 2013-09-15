// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

package tasks_test

import (
  "errors"
  "github.com/keep94/tasks"
  "github.com/keep94/tasks/recurring"
  "testing"
  "time"
)

var (
  kNow = time.Date(2013, 9, 12, 17, 21, 0, 0, time.Local)
  kSomeError = errors.New("some error")
)

func TestParallel(t *testing.T) {
  testTasks := make([]tasks.Task, 20)
  for i := range testTasks {
    testTasks[i] = &hasRunTask{}
  }
  e := tasks.Start(tasks.ParallelTasks(testTasks...))
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
  e := tasks.Start(longTask)
  if e.IsEnded() {
    t.Error("Expected IsEnded() to be false.")
  }
  e.End()
  if !e.IsEnded() {
    t.Error("Expected IsEnded() to be true.")
  }
  <-e.Done()
  if !longTask.hasRun {
    t.Error("Expected task to be run.")
  }
}

func TestNoError(t *testing.T) {
  eTask := &errorTask{}
  e := tasks.Start(eTask)
  <-e.Done()
  if e.Error() != nil {
    t.Error("Expected no error.")
  }
}

func TestNoError2(t *testing.T) {
  eTask := &errorTask{}
  if err := tasks.Run(eTask); err != nil {
    t.Error("Expected no error.")
  }
}

func TestError(t *testing.T) {
  eTask := &errorTask{kSomeError}
  e := tasks.Start(eTask)
  <-e.Done()
  if e.Error() != kSomeError {
    t.Error("Expected some error.")
  }
}

func TestError2(t *testing.T) {
  eTask := &errorTask{kSomeError}
  if err := tasks.Run(eTask); err != kSomeError {
    t.Error("Expected some error.")
  }
}

func TestRecurring(t *testing.T) {
  timeTask := &timeStampTask{}
  r := recurring.FirstN(
      recurring.AtInterval(time.Hour),
      2)
  tasks.RunForTesting(
      tasks.RecurringTask(timeTask, r), &tasks.ClockForTesting{kNow})
  verifyTimes(
      t, timeTask.timeStamps, kNow.Add(time.Hour), kNow.Add(2 * time.Hour))
}

type hasRunTask struct {
  hasRun bool
}

func (bt *hasRunTask) Do(e *tasks.Execution) {
  bt.hasRun = true
}

type longRunningTask struct {
  hasRun bool
}

func (lt *longRunningTask) Do(e *tasks.Execution) {
  e.Sleep(time.Hour)
  lt.hasRun = true
}

type errorTask struct {
  err error
}

func (et *errorTask) Do(e *tasks.Execution) {
  e.SetError(et.err)
}

type timeStampTask struct {
  timeStamps []time.Time
}

func (tt *timeStampTask) Do(e *tasks.Execution) {
  tt.timeStamps = append(tt.timeStamps, e.Now())
}

func verifyTimes(t *testing.T, actual []time.Time, expected ...time.Time) {
  if len(actual) != len(expected) {
    t.Errorf("Expected %v timestamps, got %v", len(expected), len(actual))
    return
  }
  for i := range expected {
    if expected[i] != actual[i] {
      t.Errorf("Expected time %v at %d, got %v", expected[i], i, actual[i])
    }
  }
}
