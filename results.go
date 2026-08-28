package sink

import (
	"fmt"

	sinkv1 "github.com/liran/sink-go/api/sink/v1"
)

func decodeReadResponse(response *sinkv1.ReadResponse, count int) ([]ReadResult, error) {
	if response == nil {
		return nil, protocolError("Read", "response is empty")
	}
	if len(response.GetResults()) != count {
		message := fmt.Sprintf("returned %d results for %d operations", len(response.GetResults()), count)
		return nil, protocolError("Read", message)
	}
	results := make([]ReadResult, count)
	seen := make([]bool, count)
	for _, protoResult := range response.GetResults() {
		if protoResult == nil {
			return nil, protocolError("Read", "result is empty")
		}
		index, err := validateResultIndex("Read", protoResult.GetOperationIndex(), seen)
		if err != nil {
			return nil, err
		}
		result := ReadResult{OperationIndex: index, Status: protoResult.GetStatus()}
		switch result.Status {
		case ReadFound:
			document, documentErr := documentFromProto(protoResult.GetDocument())
			if documentErr != nil {
				return nil, protocolError("Read", fmt.Sprintf("result %d: %v", index, documentErr))
			}
			result.Document = document
			result.Revision = revisionFromProto(protoResult.GetRevision())
		case ReadNotFound:
		case ReadFailed:
			failure, failureErr := operationFailure(index, protoResult.GetFailure())
			if failureErr != nil {
				return nil, protocolError("Read", failureErr.Error())
			}
			result.Failure = failure
		default:
			message := fmt.Sprintf("result %d has unsupported status %s", index, result.Status)
			return nil, protocolError("Read", message)
		}
		results[index] = result
	}
	return results, nil
}

func decodeWriteResponse(response *sinkv1.WriteResponse, count int) ([]WriteResult, error) {
	if response == nil {
		return nil, protocolError("Write", "response is empty")
	}
	if len(response.GetResults()) != count {
		message := fmt.Sprintf("returned %d results for %d operations", len(response.GetResults()), count)
		return nil, protocolError("Write", message)
	}
	results := make([]WriteResult, count)
	seen := make([]bool, count)
	for _, protoResult := range response.GetResults() {
		if protoResult == nil {
			return nil, protocolError("Write", "result is empty")
		}
		index, err := validateResultIndex("Write", protoResult.GetOperationIndex(), seen)
		if err != nil {
			return nil, err
		}
		result := WriteResult{
			OperationIndex: index,
			Status:         protoResult.GetStatus(),
			Revision:       revisionFromProto(protoResult.GetRevision()),
		}
		switch result.Status {
		case WriteAccepted, WriteApplied:
		case WritePreconditionFailed, WriteFailed:
			failure, failureErr := operationFailure(index, protoResult.GetFailure())
			if failureErr != nil {
				return nil, protocolError("Write", failureErr.Error())
			}
			result.Failure = failure
		default:
			message := fmt.Sprintf("result %d has unsupported status %s", index, result.Status)
			return nil, protocolError("Write", message)
		}
		results[index] = result
	}
	return results, nil
}

func decodeDeleteResponse(response *sinkv1.DeleteResponse, count int) ([]DeleteResult, error) {
	if response == nil {
		return nil, protocolError("Delete", "response is empty")
	}
	if len(response.GetResults()) != count {
		message := fmt.Sprintf("returned %d results for %d operations", len(response.GetResults()), count)
		return nil, protocolError("Delete", message)
	}
	results := make([]DeleteResult, count)
	seen := make([]bool, count)
	for _, protoResult := range response.GetResults() {
		if protoResult == nil {
			return nil, protocolError("Delete", "result is empty")
		}
		index, err := validateResultIndex("Delete", protoResult.GetOperationIndex(), seen)
		if err != nil {
			return nil, err
		}
		result := DeleteResult{OperationIndex: index, Status: protoResult.GetStatus()}
		switch result.Status {
		case DeleteAccepted, DeleteApplied:
		case DeleteFailed:
			failure, failureErr := operationFailure(index, protoResult.GetFailure())
			if failureErr != nil {
				return nil, protocolError("Delete", failureErr.Error())
			}
			result.Failure = failure
		default:
			message := fmt.Sprintf("result %d has unsupported status %s", index, result.Status)
			return nil, protocolError("Delete", message)
		}
		results[index] = result
	}
	return results, nil
}

func validateResultIndex(method string, rawIndex uint32, seen []bool) (int, error) {
	if uint64(rawIndex) >= uint64(len(seen)) {
		message := fmt.Sprintf("result operation index %d is out of range", rawIndex)
		return 0, protocolError(method, message)
	}
	index := int(rawIndex)
	if seen[index] {
		message := fmt.Sprintf("result operation index %d is duplicated", index)
		return 0, protocolError(method, message)
	}
	seen[index] = true
	return index, nil
}

func operationFailure(index int, failure *sinkv1.Failure) (*OperationError, error) {
	if failure == nil {
		return nil, fmt.Errorf("result %d is missing failure details", index)
	}
	if failure.GetCode() == sinkv1.FailureCode_FAILURE_CODE_UNSPECIFIED {
		return nil, fmt.Errorf("result %d has an unspecified failure code", index)
	}
	if failure.GetMessage() == "" {
		return nil, fmt.Errorf("result %d has an empty failure message", index)
	}
	operationError := &OperationError{
		OperationIndex: index,
		Code:           failure.GetCode(),
		Message:        failure.GetMessage(),
		Retryable:      failure.GetRetryable(),
	}
	return operationError, nil
}

func protocolError(method string, message string) *ProtocolError {
	err := &ProtocolError{Method: method, Message: message}
	return err
}
