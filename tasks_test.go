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
    testTasks[i] = &fakeTask{}
  }
  e := tasks.Start(tasks.ParallelTasks(testTasks...))
  <-e.Done()

  // Blocking here is not necessary in production code. Just testing that
  // this channel gets closed too.
  <-e.Ended()
  for _, atask := range testTasks {
    ft := atask.(*fakeTask)
    if !ft.hasRun() {
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
      testTasks[i] = &fakeTask{err: kSomeError}
    } else {
      testTasks[i] = &fakeTask{}
    }
  }
  e := tasks.Start(tasks.SeriesTasks(testTasks...))
  <-e.Done()

  // First 2 tasks should have been but not 3rd task
  for i, atask := range testTasks {
    ft := atask.(*fakeTask)
    if i < 2 {
      if !ft.hasRun() {
        t.Errorf("Expected task %d to be run.", i)
      }
    } else {
      if ft.hasRun() {
        t.Errorf("Expected task %d not to be run.", i)
      }
    }
  }
}

func TestRepeatingTask(t *testing.T) {
  task := &fakeTask{}
  e := tasks.Start(tasks.RepeatingTask(task, 5))
  <-e.Done()
  if task.timesRun != 5 {
    t.Errorf("Expected 5, got %v", task.timesRun)
  }
}

func TestRepeatingTaskEnded(t *testing.T) {
  task := &fakeTask{runDuration: time.Hour}
  e := tasks.Start(tasks.RepeatingTask(task, 5))
  e.End()
  <-e.Done()
  if task.timesRun != 1 {
    t.Errorf("Expected 1, got %v", task.timesRun)
  }
}

func TestRepeatingTaskError(t *testing.T) {
  task := &fakeTask{err: kSomeError}
  e := tasks.Start(tasks.RepeatingTask(task, 5))
  <-e.Done()
  if task.timesRun != 1 {
    t.Errorf("Expected 1, got %v", task.timesRun)
  }
}

func TestEndTask(t *testing.T) {
  longTask := &fakeTask{runDuration: time.Hour}
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
  if !longTask.hasRun() {
    t.Error("Expected task to be run.")
  }
}

func TestEndTaskSeries(t *testing.T) {
  // two tasks
  testTasks := make([]tasks.Task, 2)

  for i := range testTasks {
    testTasks[i] = &fakeTask{runDuration: time.Hour}
  }
  e := tasks.Start(tasks.SeriesTasks(testTasks...))
  e.End()
  <-e.Done()

  // 2nd task should not be reached.
  for i, atask := range testTasks {
    ft := atask.(*fakeTask)
    if i < 1 {
      if !ft.hasRun() {
        t.Errorf("Expected task %d to be run.", i)
      }
    } else {
      if ft.hasRun() {
        t.Errorf("Expected task %d not to be run.", i)
      }
    }
  }
}

func TestNoError(t *testing.T) {
  eTask := &fakeTask{}
  e := tasks.Start(eTask)
  <-e.Done()
  if e.Error() != nil {
    t.Error("Expected no error.")
  }
}

func TestNoError2(t *testing.T) {
  eTask := &fakeTask{}
  if err := tasks.Run(eTask); err != nil {
    t.Error("Expected no error.")
  }
}

func TestError(t *testing.T) {
  eTask := &fakeTask{err: kSomeError}
  e := tasks.Start(eTask)
  <-e.Done()
  if e.Error() != kSomeError {
    t.Error("Expected some error.")
  }
}

func TestError2(t *testing.T) {
  eTask := &fakeTask{err: kSomeError}
  if err := tasks.Run(eTask); err != kSomeError {
    t.Error("Expected some error.")
  }
}

func TestRecurring(t *testing.T) {
  timeTask := &fakeTask{}
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
  timeTask := &fakeTask{runDuration: time.Hour}
  r := recurring.FirstN(
      recurring.AtInterval(time.Hour),
      3)
  tasks.RunForTesting(
      tasks.RecurringTask(timeTask, r), &tasks.ClockForTesting{kNow})
  verifyTimes(
      t, timeTask.timeStamps, kNow.Add(time.Hour), kNow.Add(3 * time.Hour))
}

func TestRecurringError(t *testing.T) {
  timeTask := &fakeTask{err: kSomeError}
  r := recurring.FirstN(
      recurring.AtInterval(time.Hour),
      3)
  tasks.RunForTesting(
      tasks.RecurringTask(timeTask, r), &tasks.ClockForTesting{kNow})
  verifyTimes(
      t, timeTask.timeStamps,
      kNow.Add(time.Hour))
}

func TestSimpleExecutorStart(t *testing.T) {
  task1 := &fakeTask{runDuration: time.Second, message: "task 1"}
  task2 := &fakeTask{runDuration: time.Second, message: "task 2"}
  task3 := &fakeTask{runDuration: time.Second, message: "task 3"}
  se := tasks.NewSimpleExecutor()
  now := time.Now()
  se.Start(task1)
  if tk, _ := se.Current(); tk.(*fakeTask).message != "task 1" {
    t.Error("Expect Current to be task 1.")
  }
  se.Start(task2)
  if tk, _ := se.Current(); tk.(*fakeTask).message != "task 2" {
    t.Error("Expect Current to be task 2.")
  }
  e := se.Start(task3)
  if tk, _ := se.Current(); tk.(*fakeTask).message != "task 3" {
    t.Error("Expect Current to be task 3.")
  }
  <-e.Done()
  elapsed := time.Now().Sub(now)
  if elapsed < 3 * time.Second {
    t.Error("Tasks should have been run one at a time.")
  }
  if !task1.hasRun() || !task2.hasRun() || !task3.hasRun() {
    t.Error("All three tasks should have run.")
  }
}

func TestSimpleExecutorStart2(t *testing.T) {
  task1 := &fakeTask{runDuration: time.Hour}
  task2 := &fakeTask{runDuration: time.Hour}
  task3 := &fakeTask{runDuration: time.Hour}
  se := tasks.NewSimpleExecutor()
  e := se.Start(task1)
  e.End()
  <-e.Done()
  e = se.Start(task2)
  e.End()
  <-e.Done()
  e = se.Start(task3)
  e.End()
  <-e.Done()
  if !task1.hasRun() || !task2.hasRun() || !task3.hasRun() {
    t.Error("All three tasks should have run.")
  }
}

func TestSimpleExecutorMaybeStart(t *testing.T) {
  task1 := &fakeTask{runDuration: time.Hour}
  task2 := &fakeTask{runDuration: time.Hour}
  task3 := &fakeTask{runDuration: time.Hour}
  se := tasks.NewSimpleExecutor()
  e := se.MaybeStart(task1)
  if ex := se.MaybeStart(task2); ex != nil {
    t.Error("Expected MaybeStart to return nil.")
  }
  e.End()
  <-e.Done()
  e = se.MaybeStart(task3)
  e.End()
  <-e.Done()
  if !task1.hasRun() || task2.hasRun() || !task3.hasRun() {
    t.Error("Only first and third task should have run.")
  }
}

func TestForceStart(t *testing.T) {
  task1 := &fakeTask{runDuration: time.Hour}
  task2 := &fakeTask{runDuration: time.Hour}
  task3 := &fakeTask{runDuration: time.Hour}
  se := tasks.NewSimpleExecutor()
  e1 := se.ForceStart(task1)
  e2 := se.ForceStart(task2)
  e3 := se.ForceStart(task3)
  e3.End()
  <-e1.Done()
  <-e2.Done()
  <-e3.Done()
  if !task1.hasRun() || !task2.hasRun() || !task3.hasRun() {
    t.Error("All three tasks should have run.")
  }
}

type fakeTask struct {
  runDuration time.Duration // How long task should take to run.
  err error               // the error task is to report.
  message string          // arbitrary string
  timeStamps []time.Time  // times when task was started
  timesRun int            // Number of completed runs.
}

func (ft *fakeTask) Do(e *tasks.Execution) {
  ft.timeStamps = append(ft.timeStamps, e.Now())
  if ft.err != nil {
    e.SetError(ft.err)
  }
  if ft.runDuration > 0 {
    e.Sleep(ft.runDuration)
  }
  ft.timesRun++
}

func (ft *fakeTask) hasRun() bool {
  return ft.timesRun > 0
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

