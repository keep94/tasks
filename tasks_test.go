// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

package tasks

import (
  "testing"
  "time"
)

var (
  kNow = time.Date(2013, 9, 12, 17, 21, 0, 0, time.Local)
)

func TestParallel(t *testing.T) {
  testTasks := make([]Task, 20)
  for i := range testTasks {
    testTasks[i] = &boolTask{}
  }
  e := Start(ParallelTasks(testTasks...))
  <-e.Ended()
  for _, atask := range testTasks {
    bt := atask.(*boolTask)
    if !bt.hasRun {
      t.Error("Expected task to be run.")
    }
  }
}

type boolTask struct {
  hasRun bool
}

func (bt *boolTask) Do(e *Execution) error {
  bt.hasRun = true
  return nil
}
  
