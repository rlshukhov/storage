// SPDX-License-Identifier: MPL-2.0

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package errors

import "errors"

var (
	NotFound error = errors.New("not found")
)

func Is(err, target error) bool {
	return errors.Is(err, target)
}

func NewNotFound(parentError error) error {
	return errors.Join(NotFound, parentError)
}
