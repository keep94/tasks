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
  verifyTimes(
      t,
      r.ForTime(kNow),
      firstTime,
      firstTime.Add(24 * time.Hour),
      firstTime.Add(48 * time.Hour))

  r = recurring.FirstN(recurring.AtTime(17, 22), 3)
  firstTime = time.Date(2013, 9, 12, 17, 22, 0, 0, time.Local)
  verifyTimes(
      t,
      r.ForTime(kNow),
      firstTime,
      firstTime.Add(24 * time.Hour),
      firstTime.Add(48 * time.Hour))
}

func TestOnDate(t *testing.T) {
  aDate := time.Date(2013, 9, 12, 17, 22, 0, 0, time.Local)
  r := recurring.OnDate(aDate)
  verifyTimes(
      t,
      r.ForTime(kNow),
      aDate)

  aDate = time.Date(2013, 9, 12, 17, 21, 0, 0, time.Local)
  r = recurring.OnDate(aDate)
  verifyTimes(
      t,
      r.ForTime(kNow))
}

func TestAfter(t *testing.T) {
  r := recurring.FirstN(recurring.AtTime(14, 22), 2)
  r = recurring.After(r, 3 * time.Hour)
  firstTime := time.Date(2013, 9, 12, 17, 22, 0, 0, time.Local)
  verifyTimes(
      t,
      r.ForTime(kNow),
      firstTime,
      firstTime.Add(24 * time.Hour))

  r = recurring.FirstN(recurring.AtTime(14, 21), 2)
  r = recurring.After(r, 3 * time.Hour)
  firstTime = time.Date(2013, 9, 13, 17, 21, 0, 0, time.Local)
  verifyTimes(
      t,
      r.ForTime(kNow),
      firstTime,
      firstTime.Add(24 * time.Hour))
}

func TestAtInterval(t *testing.T) {
  r := recurring.FirstN(recurring.AtInterval(time.Hour), 3)
  verifyTimes(
      t,
      r.ForTime(kNow),
      kNow.Add(time.Hour),
      kNow.Add(2 * time.Hour),
      kNow.Add(3 * time.Hour))
}

func TestCombine(t *testing.T) {
  r := recurring.Combine(
      recurring.FirstN(recurring.AtInterval(2 * time.Hour), 4),
      recurring.FirstN(recurring.AtInterval(3 * time.Hour), 4))
  verifyTimes(
      t,
      r.ForTime(kNow),
      kNow.Add(2 * time.Hour),
      kNow.Add(3 * time.Hour),
      kNow.Add(4 * time.Hour),
      kNow.Add(6 * time.Hour),
      kNow.Add(8 * time.Hour),
      kNow.Add(9 * time.Hour),
      kNow.Add(12 * time.Hour))
}

func TestCombine2(t *testing.T) {
  r := recurring.Combine(
      recurring.FirstN(recurring.AtInterval(2 * time.Hour), 0),
      recurring.FirstN(recurring.AtInterval(3 * time.Hour), 0))
  verifyTimes(
      t,
      r.ForTime(kNow))
}

func verifyTimes(t *testing.T, s functional.Stream, expectedTimes ...time.Time) {
  var actual time.Time
  for _, expected := range expectedTimes {
    if s.Next(&actual) != nil {
      t.Error("Stream too short.")
      return
    }
    if actual != expected {
      t.Errorf("Expected %v, got %v", expected, actual)
    }
  }
  if s.Next(&actual) != functional.Done {
    t.Error("Stream too long.")
  }
}
