// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

package tasks_test

import (
  "errors"
  "github.com/keep94/gofunctional3/functional"
  "github.com/keep94/tasks"
  "github.com/keep94/tasks/recurring"
  "sync"
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

func TestParallelEnded(t *testing.T) {
  testTasks := make([]tasks.Task, 20)
  for i := range testTasks {
    testTasks[i] = &fakeTask{runDuration: time.Hour}
  }
  e := tasks.Start(tasks.ParallelTasks(testTasks...))
  e.End()
  <-e.Done()
  for _, atask := range testTasks {
    ft := atask.(*fakeTask)
    if !ft.hasRun() {
      t.Error("Expected task to be run.")
    }
  }
}

func TestParallelError(t *testing.T) {
  testTasks := make([]tasks.Task, 20)
  for i := range testTasks {
    if i == 5 {
      testTasks[i] = &fakeTask{err: kSomeError}
    } else {
      testTasks[i] = &fakeTask{}
    }
  }
  e := tasks.Start(tasks.ParallelTasks(testTasks...))
  <-e.Done()
  if e.Error() != kSomeError {
    t.Error("Expected to get an error.")
  }
}

func TestParallelZeroOrOne(t *testing.T) {
  task := &fakeTask{}
  if tasks.ParallelTasks() != tasks.NilTask() {
    t.Error("Expected zero parallel tasks to be nil task.")
  }
  if tasks.ParallelTasks(task) != task {
    t.Error("Expected one parallel task to be that task.")
  }
}

func TestSeries(t *testing.T) {
  // three tasks
  testTasks := make([]tasks.Task, 3)

  // second task throws an error
  for i := range testTasks {
    testTasks[i] = &fakeTask{}
  }
  e := tasks.Start(tasks.SeriesTasks(testTasks...))
  <-e.Done()
  for i, atask := range testTasks {
    ft := atask.(*fakeTask)
    if !ft.hasRun() {
      t.Errorf("Expected task %d to be run.", i)
    }
  }
}

func TestSeriesEnded(t *testing.T) {
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

func TestSeriesError(t *testing.T) {
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

func TestSeriesZeroOrOne(t *testing.T) {
  task := &fakeTask{}
  if tasks.SeriesTasks() != tasks.NilTask() {
    t.Error("Expected zero series tasks to be nil task.")
  }
  if tasks.SeriesTasks(task) != task {
    t.Error("Expected one series task to be that task.")
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

func TestRepeatingZeroOrOne(t *testing.T) {
  task := &fakeTask{}
  if tasks.RepeatingTask(task, 0) != tasks.NilTask() {
    t.Error("Expect zero repeating task to be nil task.")
  }
  if tasks.RepeatingTask(task, 1) != task {
    t.Error("Expected one repeating task to be that task.")
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

func TestNoError(t *testing.T) {
  eTask := tasks.NilTask()
  e := tasks.Start(eTask)
  <-e.Done()
  if e.Error() != nil {
    t.Error("Expected no error.")
  }
}

func TestNoError2(t *testing.T) {
  eTask := tasks.NilTask()
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
  r := recurring.Until(
      recurring.AtInterval(kNow, time.Hour), kNow.Add(4 * time.Hour))
  tasks.RunForTesting(
      tasks.RecurringTask(timeTask, r), &tasks.ClockForTesting{kNow})
  verifyTimes(
      t, timeTask.timeStamps,
      kNow.Add(time.Hour),
      kNow.Add(2 * time.Hour),
      kNow.Add(3 * time.Hour))
}

func TestRecurringCloseStream(t *testing.T) {
  task := tasks.NilTask()
  r := &testForClose{}
  tasks.RunForTesting(
      tasks.RecurringTask(task, r), &tasks.ClockForTesting{kNow})
  if !r.closeCalled {
    t.Error("Expected close to be called.")
  }
}

func TestRecurringEnded(t *testing.T) {
  tk := tasks.NilTask()
  r := recurring.AtInterval(kNow, time.Hour)
  e := tasks.Start(tasks.RecurringTask(tk, r))
  e.End()
  <-e.Done()
}

func TestRecurringOverrun(t *testing.T) {
  timeTask := &fakeTask{runDuration: time.Hour}
  r := recurring.Until(
      recurring.AtInterval(kNow, time.Hour), kNow.Add(5 * time.Hour))
  tasks.RunForTesting(
      tasks.RecurringTask(timeTask, r), &tasks.ClockForTesting{kNow})
  verifyTimes(
      t, timeTask.timeStamps, kNow.Add(time.Hour), kNow.Add(3 * time.Hour))
}

func TestRecurringError(t *testing.T) {
  timeTask := &fakeTask{err: kSomeError}
  r := recurring.AtInterval(kNow, time.Hour)
  tasks.RunForTesting(
      tasks.RecurringTask(timeTask, r), &tasks.ClockForTesting{kNow})
  verifyTimes(
      t, timeTask.timeStamps,
      kNow.Add(time.Hour))
}

func TestSingleExecutorStart(t *testing.T) {
  task1 := &fakeTask{runDuration: time.Millisecond}
  task2 := &fakeTask{runDuration: time.Millisecond}
  task3 := &fakeTask{runDuration: time.Millisecond}
  se := tasks.NewSingleExecutor()
  defer se.Close()
  e := se.Start(task1)
  if tk, ex := se.Current(); tk.(*fakeTask) != task1 || ex != e {
    t.Error("Expect Current to be task 1.")
  }
  <-e.Done()
  e = se.Start(task2)
  if tk, ex := se.Current(); tk.(*fakeTask) != task2 || ex != e {
    t.Error("Expect Current to be task 2.")
  }
  <-e.Done()
  e = se.Start(task3)
  if tk, ex := se.Current(); tk.(*fakeTask) != task3 ||  ex != e {
    t.Error("Expect Current to be task 3.")
  }
  <-e.Done()
  time.Sleep(time.Millisecond)
  if tk, ex := se.Current(); tk != nil || ex != nil {
    t.Error("Expected current task and execution to be nil.")
  }
  if !task1.hasRun() || !task2.hasRun() || !task3.hasRun() {
    t.Error("All three tasks should have run.")
  }
}

func TestSingleExecutorForceStart(t *testing.T) {
  task1 := &fakeTask{runDuration: time.Hour}
  task2 := &fakeTask{runDuration: time.Hour}
  task3 := &fakeTask{runDuration: time.Hour}
  se := tasks.NewSingleExecutor()
  defer se.Close()
  e1 := se.Start(task1)
  e2 := se.Start(task2)
  e3 := se.Start(task3)
  e3.End()
  <-e1.Done()
  <-e2.Done()
  <-e3.Done()
  if !task1.hasRun() || !task2.hasRun() || !task3.hasRun() {
    t.Error("All three tasks should have run.")
  }
}

func TestSingleExecutorMultiThread(t *testing.T) {
  fakeTasks := make([]*fakeTask, 20)
  for i := range fakeTasks {
    fakeTasks[i] = &fakeTask{}
  }
  var wg sync.WaitGroup
  wg.Add(len(fakeTasks))
  se := tasks.NewSingleExecutor()
  defer se.Close()
  for i := range fakeTasks {
    go func(t tasks.Task) {
      e := se.Start(t)
      <-e.Done()
      wg.Done()
    }(fakeTasks[i])
  }
  wg.Wait()
  for i := range fakeTasks {
    if fakeTasks[i].timesRun != 1 {
      t.Error("Expected each task to be run exactly once.")
    }
  }
}

func TestSingleExecutorClose(t *testing.T) {
  task1 := &fakeTask{runDuration: time.Hour}
  se := tasks.NewSingleExecutor()
  e := se.Start(task1)
  se.Close()
  <-e.Done()
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

type testForClose struct {
  closeCalled bool
}

func (r *testForClose) ForTime(t time.Time) functional.Stream {
  return r
}

func (r *testForClose) Next(ptr interface{}) error {
  return functional.Done
}

func (r *testForClose) Close() error {
  r.closeCalled = true
  return nil
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

