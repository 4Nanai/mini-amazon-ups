package worldups

import (
	"log/slog"
	"mini-amazon-ups/proto"
)

type UpsWorldHandler struct {
	completionHandler  func(seqNum int64, truckid int32, x int32, y int32, status string)
	deliveredHandler   func(seqNum int64, truckid int32, packageid int64)
	truckStatusHandler func(seqNum int64, truckid int32, status string, x int32, y int32)
	errHandler         func(seqNum int64, originseqnum int64, err string)
}

func NewDefaultUpsWorldHandler() *UpsWorldHandler {
	return &UpsWorldHandler{}
}

func NewUpsWorldHandler(
	completionHandler func(seqNum int64, truckid int32, x int32, y int32, status string),
	deliveredHandler func(seqNum int64, truckid int32, packageid int64),
	truckStatusHandler func(seqNum int64, truckid int32, status string, x int32, y int32),
	errHandler func(seqNum int64, originseqnum int64, err string),
) *UpsWorldHandler {
	return &UpsWorldHandler{
		completionHandler:  completionHandler,
		deliveredHandler:   deliveredHandler,
		truckStatusHandler: truckStatusHandler,
		errHandler:         errHandler,
	}
}

func (h *UpsWorldHandler) HandleCompletion(resp *proto.UFinished) {
	if h.completionHandler != nil {
		h.completionHandler(resp.GetSeqnum(), resp.GetTruckid(), resp.GetX(), resp.GetY(), resp.GetStatus())
	} else {
		defaultCompletionHandler(resp.GetSeqnum(), resp.GetTruckid(), resp.GetX(), resp.GetY(), resp.GetStatus())
	}
}

func (h *UpsWorldHandler) HandleDelivered(resp *proto.UDeliveryMade) {
	if h.deliveredHandler != nil {
		h.deliveredHandler(resp.GetSeqnum(), resp.GetTruckid(), resp.GetPackageid())
	} else {
		defaultDeliveredHandler(resp.GetSeqnum(), resp.GetTruckid(), resp.GetPackageid())
	}
}

func (h *UpsWorldHandler) HandleTruckStatus(resp *proto.UTruck) {
	if h.truckStatusHandler != nil {
		h.truckStatusHandler(resp.GetSeqnum(), resp.GetTruckid(), resp.GetStatus(), resp.GetX(), resp.GetY())
	} else {
		defaultTruckStatusHandler(resp.GetSeqnum(), resp.GetTruckid(), resp.GetStatus(), resp.GetX(), resp.GetY())
	}
}

func (h *UpsWorldHandler) HandleErr(resp *proto.UErr) {
	if h.errHandler != nil {
		h.errHandler(resp.GetSeqnum(), resp.GetOriginseqnum(), resp.GetErr())
	} else {
		defaultErrHandler(resp.GetSeqnum(), resp.GetOriginseqnum(), resp.GetErr())
	}
}

func defaultCompletionHandler(seqNum int64, truckid int32, x int32, y int32, status string) {
	slog.Info("Received UFinished response", "seqnum", seqNum, "truckid", truckid, "x", x, "y", y, "status", status)
}

func defaultDeliveredHandler(seqNum int64, truckid int32, packageid int64) {
	slog.Info("Received UDeliveryMade response", "seqnum", seqNum, "truckid", truckid, "packageid", packageid)
}

func defaultTruckStatusHandler(seqNum int64, truckid int32, status string, x int32, y int32) {
	slog.Info("Received UTruck response", "seqnum", seqNum, "truckid", truckid, "status", status, "x", x, "y", y)
}

func defaultErrHandler(seqNum int64, originSeqNum int64, err string) {
	slog.Error("Received error from world simulator", "seqnum", seqNum, "originseqnum", originSeqNum, "error", err)
}
