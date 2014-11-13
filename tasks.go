// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

// Package tasks handles tasks that can be started and stopped
package tasks

import (
  "github.com/keep94/common"
  "github.com/keep94/gofunctional3/functional"
  "github.com/keep94/tasks/recurring"
  "sync"
  "time"
)

var (
  kPauseTask Task = pauseTask{}
  kResumeTask Task = resumeTask{}
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
  g *gate
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
      Clock: clock,
      done: make(chan struct{}),
      ended: make(chan struct{}),
      g: newGate(false)}
  defer execution.finalize()
  execution.g.Arrive()
  defer execution.g.Leave()
  task.Do(execution)
  return execution.Error()
}

// Start starts a task in a separate goroutine and returns immediately.
// Start returns that particular execution of the task.
func Start(task Task) *Execution {
  return startForPause(task, nil, false)
}

func startForPause(task Task, cleanup func(Task), paused bool) *Execution {
  execution := &Execution{
      Clock: systemClock{},
      done: make(chan struct{}),
      ended: make(chan struct{}),
      g: newGate(paused)}
  go func() {
    defer execution.finalize()
    if cleanup != nil {
      defer cleanup(task)
    }
    execution.g.Arrive()
    defer execution.g.Leave()
    paused, ended := execution.g.Wait()
    if paused && ended {
      return
    }
    task.Do(execution)
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
  e._end()
  e.g.End()
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
  return isUnblocked(e.done)
}

// IsEnded returns true if this execution has been signaled to end.
func (e *Execution) IsEnded() bool {
  return isUnblocked(e.ended)
}

// Sleep sleeps for the specified duration or until this execution should
// end, whichever comes first. Sleep returns false if it returned early
// because this execution should end; otherwise it returns true.
// If paused, the sleep timer continues to run normally; however, after the
// sleep timer runs out, Sleep will continue to block until either this
// execution is signaled to end (in which case it returns false) 
// or unpaused (in which case it returns true).
func (e *Execution) Sleep(d time.Duration) bool {
  return e.Yield(func() {
    e.sleep(d)
  })
}

// Yield runs sleepFunc and after that returns true unless this exeuction
// was signaled to end in which case it returns false. sleepFunc can be nil.
// If paused, sleepFunc continues to run uninterrupted; however, after 
// sleepFunc returns, Yield will continue to block until either this
// execution is signaled to end (in which case it returns false)
// or unpaused (in which case it returns true).
func (e *Execution) Yield(sleepFunc func()) bool {
  return e.g.Yield(sleepFunc)
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

func (e *Execution) _end() {
  if e.markEnded() {
    close(e.ended)
  }
}

func (e *Execution) markEnded() bool {
  e.lock.Lock()
  defer e.lock.Unlock()
  result := !e.bEnded
  e.bEnded = true
  return result
}

func (e *Execution) sleep(d time.Duration) {
  select {
    case <-e.ended:
    case <-e.After(d):
  }
}

func (e *Execution) finalize() {
  e._end()
  close(e.done)
}

func (e *Execution) pause() {
  e.g.Shut()
}

func (e *Execution) resume() {
  e.g.Open()
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
  var aggregate parallelTasks
  return common.Join(tasks, aggregate, nilTask{}).(Task)
}

// SeriesTasks returns a task that performas all the passed in tasks in
// series. If one of the tasks reports an error, the others following it
// don't get executed.
func SeriesTasks(tasks ...Task) Task {
  var aggregate seriesTasks
  return common.Join(tasks, aggregate, nilTask{}).(Task)
}

// RepeatingTask returns a task that performs the pased in task n times.
func RepeatingTask(t Task, n int) Task {
  switch {
    case n <= 0:
      return nilTask{}
    case n == 1:
      return t
    default:
      if nested, ok := t.(*repeatingTask); ok {
        return &repeatingTask{t: nested.t, n: nested.n * n}
      }
      return &repeatingTask{t: t, n: n}
  }
}

// NilTask returns a task that does nothing.
func NilTask() Task {
  return nilTask{}
}

// ClockForTesting is a test implementation of Clock.
// Unlike the real clock, current time remains the same unless client changes
// it directly or calls After()
type ClockForTesting struct {

  // The current time
  Current time.Time
}

func (c *ClockForTesting) Now() time.Time {
  return c.Current
}

// After immediately advances current time by d and sends that currnet time
// on the returned channel.
func (c *ClockForTesting) After(d time.Duration) <-chan time.Time {
  c.Current = c.Current.Add(d)
  result := make(chan time.Time, 1)
  result <- c.Current
  close(result)
  return result
}

// SingleExecutor executes tasks one at a time.
// SingleExecutor instances are safe to use with multiple goroutines.
// Tasks used with SingleExecutor must support equality. For instance, the
// underlying type used for Task could be a pointer type.
// Clients should consider SingleExecutor and MultiExecutor using the same
// underlying type an implementation detail that could change in the future. 
type SingleExecutor MultiExecutor

// NewSingleExecutor returns a new SingleExecutor.
func NewSingleExecutor() *SingleExecutor {
  return (*SingleExecutor)(NewMultiExecutor(&singleTaskCollection{}))
}

// Start starts task t and returns its Execution. Start blocks until this
// instance actually starts t. Start interrupts any currently running task
// before starting t.
func (se *SingleExecutor) Start(t Task) *Execution {
  return (*MultiExecutor)(se).Start(t)
}

// Pause pauses this executor. Pause blocks until all running tasks in this
// executor have either ended or called Yield or Sleep on their Execution
// instance.
// Pause() and Resume() must be called from the same goroutine.
// Calling Pause() and Resume() concurrently from different goroutines
// causes undefined behavior and may cause Pause() to block indefinitely.
func (se *SingleExecutor) Pause() {
  (*MultiExecutor)(se).Pause()
}

// Resume resumes this once paused executor by letting any in-progress
// tasks that had called Yield or Sleep on their Execution instance continue.
// Pause() and Resume() must be called from the same goroutine.
// Calling Pause() and Resume() concurrently from different goroutines
// causes undefined behavior and may cause Pause() to block indefinitely.
func (se *SingleExecutor) Resume() {
  (*MultiExecutor)(se).Resume()
}

// Current returns the current running task and its execution. If no task
// is running, Current returns nil, nil.
func (se *SingleExecutor) Current() (Task, *Execution) {
  return (*MultiExecutor)(se).Tasks().(*singleTaskCollection).Current()
}

// Close frees the resources of this instance and always returns nil. Close
// interrupts any currently running task.
func (se *SingleExecutor) Close() error {
  return (*MultiExecutor)(se).Close()
}

// Interface TaskCollection represents a collection of running tasks.
// Clients must not call the Add() or Remove() method directly.
// Implementations of this interface can provide additional
// methods giving clients a read-only view of running tasks and executions.
type TaskCollection interface {
  // Add adds a task and execution of that task to this collection.
  Add(t Task, e *Execution)

  // Remove removes task t from this collection.
  Remove(t Task)

  // Conflicts returns the execution of all tasks that conflict with t.
  // If t is nil it means return the executions of all tasks in this
  // collection.
  Conflicts(t Task) []*Execution
}

// MultiExecutor executes multiple tasks at one time while ensuring that no
// conflicting tasks execute in parallel.
// MultiExecutor is safe to use with multiple goroutines.
// Because of TaskCollection.Remove, tasks used with MultiExecutor must
// support equality. For instance, the underlying type used for Task could be
// a pointer type.
type MultiExecutor struct {
  tc TaskCollection
  taskCh chan Task
  taskRetCh chan *Execution
}
  
// NewMultiExecutor returns a new MultiExecutor. tc is the TaskCollection that
// will hold running tasks. tc shall be safe to use with multiple goroutines
// and each MultiExecutor shall have its own TaskCollection instance.
func NewMultiExecutor(tc TaskCollection) *MultiExecutor {
  result := &MultiExecutor{
      tc: tc,
      taskCh: make(chan Task),
      taskRetCh: make(chan *Execution)}
  go result.loop()
  return result
}

// Start starts task t and returns its Execution. Start blocks until this
// instance actually starts t. Start interrupts any currently running 
// conflicting tasks before starting t.
func (me *MultiExecutor) Start(t Task) *Execution {
  if t == nil {
    panic("Got a nil task.")
  }
  me.taskCh <- t
  return <-me.taskRetCh
}

// Pause pauses this executor. Pause blocks until all running tasks in this
// executor have either ended or called Yield or Sleep on their Execution
// instance.
// Pause() and Resume() must be called from the same goroutine.
// Calling Pause() and Resume() concurrently from different goroutines
// causes undefined behavior and may cause Pause() to block indefinitely.
func (me *MultiExecutor) Pause() {
  me.Start(kPauseTask)
}

// Resume resumes this once paused executor by letting any in-progress
// tasks that had called Yield or Sleep on their Execution instance continue.
// Pause() and Resume() must be called from the same goroutine.
// Calling Pause() and Resume() concurrently from different goroutines
// causes undefined behavior and may cause Pause() to block indefinitely.
func (me *MultiExecutor) Resume() {
  me.Start(kResumeTask)
}

// Tasks returns the running tasks.
func (me *MultiExecutor) Tasks() TaskCollection {
  return me.tc
}

// Close frees the resources of this instance and always returns nil. Close
// interrupts any currently running tasks.
func (me *MultiExecutor) Close() error {
  me.taskCh <- nil
  close(me.taskCh)
  interruptAll(me.tc.Conflicts(nil))
  return nil
}

func (me *MultiExecutor) loop() {
  var bPaused bool
  for {
    // Get the next task from the Start method.
    t := <-me.taskCh
    if t == nil {  // Our taskCh has been closed.
      close(me.taskRetCh)
      return
    }

    if t == kPauseTask {
      pauseAll(me.tc.Conflicts(nil))
      bPaused = true
      me.taskRetCh <- nil
      continue
    }
    if t == kResumeTask {
      resumeAll(me.tc.Conflicts(nil))
      bPaused = false
      me.taskRetCh <- nil
      continue
    }

    // Interrupt the conflicting tasks and wait for them to end.
    interruptAll(me.tc.Conflicts(t))

    // Start executing our task taking care to remove it from the collection
    // of running tasks when it completes.
    exec := startForPause(
        t,
        func(tk Task) {
          me.tc.Remove(tk)
        },
        bPaused)

    // Add our newly running task to the collection of running tasks.
    me.tc.Add(t, exec)

    // Tell Start method that we have started
    me.taskRetCh <- exec
  }
}

type singleTaskCollection struct {
  mutex sync.Mutex
  t Task
  e *Execution
}

func (stc *singleTaskCollection) Add(t Task, e *Execution) {
  stc.mutex.Lock()
  defer stc.mutex.Unlock()
  if stc.t != nil || stc.e != nil {
    panic("Trying to add a task to a full singleTaskCollection.")
  }
  stc.t = t
  stc.e = e
}

func (stc *singleTaskCollection) Remove(t Task) {
  stc.mutex.Lock()
  defer stc.mutex.Unlock()
  if stc.t == t {
    stc.t = nil
    stc.e = nil
  }
}

func (stc *singleTaskCollection) Conflicts(t Task) []*Execution {
  stc.mutex.Lock()
  defer stc.mutex.Unlock()
  if stc.e == nil {
    return nil
  }
  return []*Execution{stc.e}
}

func (stc *singleTaskCollection) Current() (Task, *Execution) {
  stc.mutex.Lock()
  defer stc.mutex.Unlock()
  return stc.t, stc.e
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
      defer wg.Done()
      e.g.Arrive()
      defer e.g.Leave()
      paused, ended := e.g.Wait()
      if paused && ended {
        return
      }
      t.Do(e)
    }(task)
  }
  e.g.Leave()
  defer e.g.Arrive()
  wg.Wait()
}

type seriesTasks []Task 

func (s seriesTasks) Do(e *Execution) {
  for _, task := range s {
    task.Do(e)
    if e.Error() != nil || !e.Yield(nil) {
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
    if e.Error() != nil || !e.Yield(nil) {
      return
    }
  }
}

type nilTask struct {
}

func (n nilTask) Do(e *Execution) {
}

type systemClock struct {
}

func (s systemClock) Now() time.Time {
  return time.Now()
}

func (s systemClock) After(d time.Duration) <-chan time.Time {
  return time.After(d)
}

type gate struct {
  lock sync.Mutex
  activeCount int
  bPaused bool
  bEnded bool
  allPaused *sync.Cond
  wakeup *sync.Cond
}

func newGate(paused bool) *gate {
  result := &gate{bPaused: paused}
  result.allPaused = sync.NewCond(&result.lock)
  result.wakeup = sync.NewCond(&result.lock)
  return result
}

func (g *gate) Arrive() {
  g.lock.Lock()
  defer g.lock.Unlock()
  g.arrive()
}

func (g *gate) Leave() {
  g.lock.Lock()
  defer g.lock.Unlock()
  g.leave()
}

func (g *gate) Open() {
  g.lock.Lock()
  defer g.lock.Unlock()
  g.bPaused = false
  g.wakeup.Broadcast()
}

func (g *gate) Shut() {
  g.lock.Lock()
  defer g.lock.Unlock()
  g.bPaused = true
  for g.activeCount > 0 {
    g.allPaused.Wait()
  }
}

func (g *gate) End() {
  g.lock.Lock()
  defer g.lock.Unlock()
  g.bEnded = true
  g.wakeup.Broadcast()
}

func (g *gate) Yield(waitFunc func()) bool {
  if waitFunc != nil {
    g.executeWaitFunc(waitFunc)
  }
  _, ended := g.Wait()
  return !ended
}

func (g *gate) Wait() (paused bool, ended bool) {
  g.lock.Lock()
  defer g.lock.Unlock()
  g.leave()
  defer g.arrive()
  for g.bPaused && !g.bEnded {
    g.wakeup.Wait()
  }
  return g.bPaused, g.bEnded
}

func (g *gate) executeWaitFunc(waitFunc func()) {
  g.Leave()
  defer g.Arrive()
  waitFunc()
}

func (g *gate) arrive() {
  g.activeCount++
}

func (g *gate) leave() {
  g.activeCount--
  if g.activeCount < 0 {
    panic("negative activeCount")
  }
  if g.activeCount == 0 {
    g.allPaused.Broadcast()
  }
}

type pauseTask struct {
}

func (p pauseTask) Do(e *Execution) {
}

type resumeTask struct {
}

func (r resumeTask) Do(e *Execution) {
}

func interruptAll(executions []*Execution) {
  for _, e := range executions {
    e.End()
  }
  for _, e := range executions {
    <-e.Done()
  }
}

func pauseAll(executions []*Execution) {
  for _, e := range executions {
    e.pause()
  }
}

func resumeAll(executions []*Execution) {
  for _, e := range executions {
    e.resume()
  }
}

func isUnblocked(ch <-chan struct{}) bool {
  select {
    case <-ch:
      return true
    default:
  }
  return false
}
