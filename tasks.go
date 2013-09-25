// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

// Package tasks handles tasks that can be started and stopped
package tasks

import (
  "github.com/keep94/gofunctional3/functional"
  "github.com/keep94/tasks/recurring"
  "sync"
  "time"
)

// Task represents any task
type Task interface {

  // Do performs the task. execution is the specific execution of this task.
  Do(execution *Execution)
}

// TaskFunc wraps a simple function to implement Task.
type TaskFunc func(execution *Execution)

func (f TaskFunc) Do(execution *Execution) {
  f(execution)
}

// Clock represents the system clock.
type Clock interface {

  // Now returns the current time
  Now() time.Time

  // After waits for given duration to elapse and then sends current time on
  // the returned channel.
  After(d time.Duration) <-chan time.Time
}

// Execution represents a particular execution of some task.
// Execution instances are safe to use with multiple goroutines.
type Execution struct {
  Clock
  ended chan struct{}
  done chan struct{}
  bEnded bool
  err error
  lock sync.Mutex
}

// Run executes a task in the current goroutine and exits when the task
// finishes.
func Run(task Task) error {
  return RunForTesting(task, systemClock{})
}

// RunForTesting work just like Run except it allows caller to specify
// an implementation of the Clock interface for testing.
func RunForTesting(task Task, clock Clock) (err error) {
  execution := &Execution{
      Clock: clock, done: make(chan struct{}), ended: make(chan struct{})}
  task.Do(execution)
  execution.End()
  close(execution.done)
  return execution.Error()
}

// Start starts a task in a separate goroutine and returns immediately.
// Start returns that particular execution of the task.
func Start(task Task) *Execution {
  execution := &Execution{
      Clock: systemClock{},
      done: make(chan struct{}),
      ended: make(chan struct{})}
  go func() {
    task.Do(execution)
    execution.End()
    close(execution.done)
  }()
  return execution
}

// Error returns error from this execution.
func (e *Execution) Error() error {
  e.lock.Lock()
  defer e.lock.Unlock()
  return e.err
}

// End signals that execution should end.
func (e *Execution) End() {
  if e.markEnded() {
    close(e.ended)
  }
}

// Ended returns a channel that gets closed when this execution is signaled
// to end.
func (e *Execution) Ended() <-chan struct{} {
  return e.ended
}

// Done returns a channel that gets closed when this execution is done.
func (e *Execution) Done() <-chan struct{} {
  return e.done
}

// IsDone returns true if this execution is done or false if it is still
// in progress.
func (e *Execution) IsDone() bool {
  select {
    case <-e.done:
      return true
    default:
      return false
  }
  return false
}

// IsEnded returns true if this execution has been signaled to end.
func (e *Execution) IsEnded() bool {
  select {
    case <-e.ended:
      return true
    default:
      return false
  }
  return false
}

// Sleep sleeps for the specified duration ends or until this execution should
// end, whichever comes first. Sleep returns true if it slept the entire
// duration or false if it returned early because this execution should end.
func (e *Execution) Sleep(d time.Duration) bool {
  select {
    case <-e.ended:
      return false
    case <-e.After(d):
      return true
  }
  return false
}

// SetError lets a task report an error.
func (e *Execution) SetError(err error) {
  if err == nil {
    return
  }
  e.lock.Lock()
  defer e.lock.Unlock()
  e.err = err
}

func (e *Execution) markEnded() bool {
  e.lock.Lock()
  defer e.lock.Unlock()
  result := !e.bEnded
  e.bEnded = true
  return result
}

// RecurringTask returns a task that does t at each time that r specifies.
// The returned task ends when there are no more times from r or if some
// error happens while executing one of the tasks.
func RecurringTask(t Task, r recurring.R) Task {
  return &recurringTask{t, r}
}

// ParallelTasks returns a task that performs all the passed in tasks in
// parallel.
func ParallelTasks(tasks ...Task) Task {
  return parallelTasks(tasks)
}

// SeriesTasks returns a task that performas all the passed in tasks in
// series. If one of the tasks reports an error, the others following it
// don't get executed.
func SeriesTasks(tasks ...Task) Task {
  return seriesTasks(tasks)
}

// RepeatingTask returns a task that performs the pased in task n times.
func RepeatingTask(t Task, n int) Task {
  return &repeatingTask{t, n}
}

// ClockForTesting is a test implementation of Clock.
// Current time advances only when After() is called.
type ClockForTesting struct {

  // The current time
  Current time.Time
}

func (c *ClockForTesting) Now() time.Time {
  return c.Current
}

// After immediately advances current time by d and send that currnet time
// on the returned channel.
func (c *ClockForTesting) After(d time.Duration) <-chan time.Time {
  c.Current = c.Current.Add(d)
  result := make(chan time.Time, 1)
  result <- c.Current
  close(result)
  return result
}

// SingleExecutor executes tasks one at a time. SingleExecutor instances are
// safe to use with multiple goroutines.
type SingleExecutor struct {
  lock sync.Mutex
  task Task
  execution *Execution
  taskCh chan Task
  taskRetCh chan *Execution
}

// NewSingleExecutor returns a new SingleExecutor.
func NewSingleExecutor() *SingleExecutor {
  result := &SingleExecutor{
      taskCh: make(chan Task), taskRetCh: make(chan *Execution)}
  go result.loop()
  return result
}

// Start starts task t and returns its Execution. Start blocks until this
// instance actually starts t. Start interrupts any currently running task
// before starting t.
func (se *SingleExecutor) Start(t Task) *Execution {
  if t == nil {
    panic("Got a nil task.")
  }
  se.taskCh <- t
  return <-se.taskRetCh
}

// Current returns the current running task and its execution. If no task
// is running, Current may return nil, nil or it may return the last run
// task along with its execution.
func (se *SingleExecutor) Current() (Task, *Execution) {
  se.lock.Lock()
  defer se.lock.Unlock()
  return se.task, se.execution
}

// Close frees the resources of this instance and always returns nil. Close
// interrupts any currently running task.
func (se *SingleExecutor) Close() error {
  close(se.taskCh)
  return nil
}

func (se *SingleExecutor) setCurrent(t Task, e *Execution) {
  se.lock.Lock()
  defer se.lock.Unlock()
  se.task, se.execution = t, e
}

func (se *SingleExecutor) loop() {
  var t Task  // The task to be run
  for {
    // If we don't already have a task to run, wait until we get one.
    if t == nil {
      t = <-se.taskCh
      if t == nil {  // Our taskCh has been closed.
        close(se.taskRetCh)
        return
      }
    }
    // Start task
    e := Start(t)
    se.setCurrent(t, e)

    // Tell Start method that we have started
    se.taskRetCh <- e

    t = nil
    // Block until current task done or until there is a new task to run
    select {
      case <-e.Done():
      case t = <-se.taskCh:
        e.End()
        <-e.Done()
        if t == nil {  // Our taskCh has been closed.
          close(se.taskRetCh)
          return
        }
    }
    se.setCurrent(nil, nil)
  }
}

type recurringTask struct {
  t Task
  r recurring.R
}

func (rt *recurringTask) Do(e *Execution) {
  s := rt.r.ForTime(e.Now())
  defer s.Close()
  var t time.Time
  var err error
  for err = s.Next(&t); err == nil; err = s.Next(&t) {
    dur := t.Sub(e.Now())
    if dur <= 0 {
      continue
    }
    if !e.Sleep(dur) {
      return
    }
    rt.t.Do(e)
    if e.Error() != nil {
      return
    }
  }
  if err != functional.Done {
    e.SetError(err)
  }
}

type parallelTasks []Task

func (p parallelTasks) Do(e *Execution) {
  var wg sync.WaitGroup
  wg.Add(len(p))
  for _, task := range p {
    go func(t Task) {
      t.Do(e)
      wg.Done()
    }(task)
  }
  wg.Wait()
}

type seriesTasks []Task 

func (s seriesTasks) Do(e *Execution) {
  for _, task := range s {
    task.Do(e)
    if e.IsEnded() || e.Error() != nil {
      return
    }
  }
}

type repeatingTask struct {
  t Task
  n int
}

func (r *repeatingTask) Do(e *Execution) {
  for i := 0; i < r.n; i++ {
    r.t.Do(e)
    if e.IsEnded() || e.Error() != nil {
      return
    }
  }
}

type systemClock struct {
}

func (s systemClock) Now() time.Time {
  return time.Now()
}

func (s systemClock) After(d time.Duration) <-chan time.Time {
  return time.After(d)
}
