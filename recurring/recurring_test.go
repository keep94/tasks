// Copyright 2013 Travis Keep. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or
// at http://opensource.org/licenses/BSD-3-Clause.

package recurring_test

import (
  "github.com/keep94/gofunctional3/functional"
  "github.com/keep94/tasks/recurring"
  "testing"
  "time"
)

var (
  kNow = time.Date(2013, 9, 12, 17, 21, 0, 0, time.Local)
)

func TestAtTime(t *testing.T) {
  r := recurring.FirstN(recurring.AtTime(17, 21), 3)
  firstTime := time.Date(2013, 9, 13, 17, 21, 0, 0, time.Local)
  verifyTimes(t, r.ForTime(kNow), firstTime, firstTime.Add(24 * time.Hour), firstTime.Add(48 * time.Hour))
  r = recurring.FirstN(recurring.AtTime(17, 22), 3)
  firstTime = time.Date(2013, 9, 12, 17, 22, 0, 0, time.Local)
  verifyTimes(t, r.ForTime(kNow), firstTime, firstTime.Add(24 * time.Hour), firstTime.Add(48 * time.Hour))
}

func TestAtInterval(t *testing.T) {
  r := recurring.FirstN(recurring.AtInterval(time.Hour), 3)
  firstTime := time.Date(2013, 9, 12, 17, 21, 0, 0, time.Local)
  verifyTimes(t, r.ForTime(kNow), firstTime, firstTime.Add(time.Hour), firstTime.Add(2 * time.Hour))
}

func verifyTimes(t *testing.T, s functional.Stream, expectedTimes ...time.Time) {
  var actual time.Time
  for _, expected := range expectedTimes {
    if s.Next(&actual) != nil {
      t.Error("Stream too short.")
    }
    if actual != expected {
      t.Errorf("Expected %v, got %v", expected, actual)
    }
  }
  if s.Next(&actual) != functional.Done {
    t.Error("Stream too long.")
  }
}
