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
  kSomeError = errors.New("tasks: some error")
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
    hrt := atask.(*hasRunTask)
    if !hrt.hasRun {
      t.Error("Expected task to be run.")
    }
  }
}

func TestSeries(t *testing.T) {
  // three tasks
  testTasks := make([]tasks.Task, 3)

  // second task throws an error
  for i := range testTasks {
    if i == 1 {
      testTasks[i] = &errorTask{err: kSomeError}
    } else {
      testTasks[i] = &errorTask{}
    }
  }
  e := tasks.Start(tasks.SeriesTasks(testTasks...))
  <-e.Done()

  // First 2 tasks should have been but not 3rd task
  for i, atask := range testTasks {
    et := atask.(*errorTask)
    if i < 2 {
      if !et.hasRun {
        t.Errorf("Expected task %d to be run.", i)
      }
    } else {
      if et.hasRun {
        t.Errorf("Expected task %d not to be run.", i)
      }
    }
  }
}

func TestRepeatingTask(t *testing.T) {
  task := &hasRunTask{}
  e := tasks.Start(tasks.RepeatingTask(task, 5))
  <-e.Done()
  if task.timesRun != 5 {
    t.Errorf("Expected 5, got %v", task.timesRun)
  }
}

func TestRepeatingTaskEnded(t *testing.T) {
  task := &longRunningTask{}
  e := tasks.Start(tasks.RepeatingTask(task, 5))
  e.End()
  <-e.Done()
  if task.timesRun != 1 {
    t.Errorf("Expected 1, got %v", task.timesRun)
  }
}

func TestRepeatingTaskError(t *testing.T) {
  task := &errorTask{err: kSomeError}
  e := tasks.Start(tasks.RepeatingTask(task, 5))
  <-e.Done()
  if task.timesRun != 1 {
    t.Errorf("Expected 1, got %v", task.timesRun)
  }
}

func TestEndTask(t *testing.T) {
  longTask := &longRunningTask{}
  e := tasks.Start(longTask)
  if e.IsEnded() {
    t.Error("Expected IsEnded() to be false.")
  }
  if e.IsDone() {
    t.Error("Expected IsDone() to be false.")
  }
  e.End()
  if !e.IsEnded() {
    t.Error("Expected IsEnded() to be true.")
  }
  <-e.Done()
  if !e.IsDone() {
    t.Error("Expected IsDone() to be true.")
  }
  if !longTask.hasRun {
    t.Error("Expected task to be run.")
  }
}

func TestEndTaskSeries(t *testing.T) {
  // two tasks
  testTasks := make([]tasks.Task, 2)

  for i := range testTasks {
    testTasks[i] = &longRunningTask{}
  }
  e := tasks.Start(tasks.SeriesTasks(testTasks...))
  e.End()
  <-e.Done()

  // 2nd task should not be reached.
  for i, atask := range testTasks {
    et := atask.(*longRunningTask)
    if i < 1 {
      if !et.hasRun {
        t.Errorf("Expected task %d to be run.", i)
      }
    } else {
      if et.hasRun {
        t.Errorf("Expected task %d not to be run.", i)
      }
    }
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
  eTask := &errorTask{err: kSomeError}
  e := tasks.Start(eTask)
  <-e.Done()
  if e.Error() != kSomeError {
    t.Error("Expected some error.")
  }
}

func TestError2(t *testing.T) {
  eTask := &errorTask{err: kSomeError}
  if err := tasks.Run(eTask); err != kSomeError {
    t.Error("Expected some error.")
  }
}

func TestRecurring(t *testing.T) {
  timeTask := &timeStampTask{}
  r := recurring.FirstN(
      recurring.AtInterval(time.Hour),
      3)
  tasks.RunForTesting(
      tasks.RecurringTask(timeTask, r), &tasks.ClockForTesting{kNow})
  verifyTimes(
      t, timeTask.timeStamps,
      kNow.Add(time.Hour),
      kNow.Add(2 * time.Hour),
      kNow.Add(3 * time.Hour))
}

func TestRecurringOverrun(t *testing.T) {
  timeTask := &timeStampTask{runDuration: time.Hour}
  r := recurring.FirstN(
      recurring.AtInterval(time.Hour),
      3)
  tasks.RunForTesting(
      tasks.RecurringTask(timeTask, r), &tasks.ClockForTesting{kNow})
  verifyTimes(
      t, timeTask.timeStamps, kNow.Add(time.Hour), kNow.Add(3 * time.Hour))
}

type hasRunTask struct {
  hasRun bool
  timesRun int
}

func (hrt *hasRunTask) Do(e *tasks.Execution) {
  hrt.hasRun = true
  hrt.timesRun++
}

type longRunningTask struct {
  hasRun bool
  timesRun int
}

func (lt *longRunningTask) Do(e *tasks.Execution) {
  e.Sleep(time.Hour)
  lt.hasRun = true
  lt.timesRun++
}

type errorTask struct {
  err error
  hasRun bool
  timesRun int
}

func (et *errorTask) Do(e *tasks.Execution) {
  e.SetError(et.err)
  et.hasRun = true
  et.timesRun++
}

type timeStampTask struct {
  runDuration time.Duration
  timeStamps []time.Time
}

func (tt *timeStampTask) Do(e *tasks.Execution) {
  tt.timeStamps = append(tt.timeStamps, e.Now())
  e.Sleep(tt.runDuration)
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

