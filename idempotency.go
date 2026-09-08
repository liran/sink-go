package sink

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// OperationID identifies one logical mutation, including its fixed creation
// time. Persist it to retry across calls/processes. It expires after 31 days.
type OperationID string

func NewOperationID() (OperationID, error) {
	identity := make([]byte, 16)
	if _, err := rand.Read(identity); err != nil {
		return "", err
	}
	return encodeOperationID(identity, time.Now()), nil
}

// OperationIDFor derives a stable ID from a business key and the operation's
// ORIGINAL creation time (not the current retry time). Use distinct keys for
// distinct steps targeting the same record.
func OperationIDFor(key string, created time.Time) (OperationID, error) {
	if key == "" || created.IsZero() {
		return "", errors.New("operation key and original creation time are required")
	}
	digest := sha256.Sum256([]byte(key))
	id := encodeOperationID(digest[:], created)
	if err := id.validate(); err != nil {
		return "", err
	}
	return id, nil
}

func encodeOperationID(identity []byte, created time.Time) OperationID {
	return OperationID("v1:" + strconv.FormatInt(created.UnixMilli(), 10) + ":" + base64.RawURLEncoding.EncodeToString(identity))
}

func (id OperationID) validate() error {
	parts := strings.Split(string(id), ":")
	if len(parts) != 3 || parts[0] != "v1" || len(id) > 160 {
		return errors.New("invalid operation ID")
	}
	millis, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || strconv.FormatInt(millis, 10) != parts[1] {
		return errors.New("invalid operation ID timestamp")
	}
	identity, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(identity) < 16 || len(identity) > 64 || base64.RawURLEncoding.EncodeToString(identity) != parts[2] {
		return errors.New("invalid operation ID identity")
	}
	created := time.UnixMilli(millis)
	now := time.Now()
	if created.After(now.Add(5*time.Minute)) || !now.Before(created.Add(31*24*time.Hour)) {
		return errors.New("operation ID expired or has a future timestamp; do not regenerate it to retry")
	}
	return nil
}

func (o WriteOperation) WithOperationID(id OperationID) WriteOperation {
	o.operationID = id
	return o
}

func (o WriteOperation) OperationID() OperationID { return o.operationID }

// WriteTransportError retains pending operations with their original IDs after
// an uncertain transport/protocol outcome. Earlier returned batches must not be
// added back. Retry Operations with the original completion mode.
type WriteTransportError struct {
	Cause      error
	Operations []WriteOperation
}

func (e *WriteTransportError) Error() string {
	return fmt.Sprintf("idempotent write outcome unavailable: %v", e.Cause)
}
func (e *WriteTransportError) Unwrap() error { return e.Cause }

func (c *Client) prepareOperationIDs(operations []WriteOperation) ([]WriteOperation, error) {
	protected := c.config.idempotentWrites
	for _, operation := range operations {
		protected = protected || operation.operationID != ""
	}
	if !protected {
		return operations, nil
	}
	prepared := append([]WriteOperation(nil), operations...)
	for index := range prepared {
		if prepared[index].operationID == "" {
			id, err := NewOperationID()
			if err != nil {
				return nil, err
			}
			prepared[index].operationID = id
		}
		if err := prepared[index].operationID.validate(); err != nil {
			return nil, fmt.Errorf("write operation %d: %w", index, err)
		}
	}
	return prepared, nil
}
