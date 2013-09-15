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
  Do(execution *Execution) error
}

// TaskFunc wraps a simple function to implement Task.
type TaskFunc func(execution *Execution) error

func (f TaskFunc) Do(execution *Execution) error {
  return f(execution)
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

func RunForTesting(task Task, clock Clock) (err error) {
  execution := &Execution{
      Clock: clock, done: make(chan struct{}), ended: make(chan struct{})}
  execution.setError(task.Do(execution))
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
    execution.setError(task.Do(execution))
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

// IsEnded returns true if this execution has been signaled to end.
func (e *Execution) IsEnded() bool {
  e.lock.Lock()
  defer e.lock.Unlock()
  return e.bEnded
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

func (e *Execution) setError(err error) {
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
// parallel. The Do method of the returned Task always returns nil. However
// if one of the individual tasks in tasks returns an error that error still
// gets stored in the Execution object.
func ParallelTasks(tasks ...Task) Task {
  return parallelTasks(tasks)
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

type recurringTask struct {
  t Task
  r recurring.R
}

func (rt *recurringTask) Do(e *Execution) (err error) {
  s := rt.r.ForTime(e.Now())
  defer s.Close()
  var t time.Time
  for err = s.Next(&t); err == nil; err = s.Next(&t) {
    dur := t.Sub(e.Now())
    if dur <= 0 {
      continue
    }
    if !e.Sleep(dur) {
      return
    }
    if err = rt.t.Do(e); err != nil {
      return
    }
  }
  if err == functional.Done {
    err = nil
  }
  return
}

type parallelTasks []Task

func (p parallelTasks) Do(e *Execution) (err error) {
  var wg sync.WaitGroup
  wg.Add(len(p))
  for _, task := range p {
    go func(t Task) {
      e.setError(t.Do(e))
      wg.Done()
    }(task)
  }
  wg.Wait()
  return
}

type systemClock struct {
}

func (s systemClock) Now() time.Time {
  return time.Now()
}

func (s systemClock) After(d time.Duration) <-chan time.Time {
  return time.After(d)
}
