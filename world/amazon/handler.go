package worldamazon

import (
	"log/slog"
	"mini-amazon-ups/proto"
)

type AmazonWorldHandler struct {
	purchaseMoreHandler func(seqNum int64, whnum int32, things []*proto.AProduct)
	packedHandler       func(seqNum int64, shipid int64)
	loadedHandler       func(seqNum int64, shipid int64)
	packageHandler      func(seqNum int64, packageid int64, status string)
	errHandler          func(seqNum int64, originseqnum int64, err string)
}

func NewDefaultAmazonWorldHandler() *AmazonWorldHandler {
	return &AmazonWorldHandler{}
}

func NewAmazonWorldHandler(
	purchaseMoreHandler func(seqNum int64, whnum int32, things []*proto.AProduct),
	packedHandler func(seqNum int64, shipid int64),
	loadedHandler func(seqNum int64, shipid int64),
	packageHandler func(seqNum int64, packageid int64, status string),
	errHandler func(seqNum int64, originseqnum int64, err string),
) *AmazonWorldHandler {
	return &AmazonWorldHandler{
		purchaseMoreHandler: purchaseMoreHandler,
		packedHandler:       packedHandler,
		loadedHandler:       loadedHandler,
		packageHandler:      packageHandler,
		errHandler:          errHandler,
	}
}

func (h *AmazonWorldHandler) HandlePurchaseMore(resp *proto.APurchaseMore) {
	if h.purchaseMoreHandler != nil {
		h.purchaseMoreHandler(resp.GetSeqnum(), resp.GetWhnum(), resp.GetThings())
	} else {
		defaultPurchaseMoreHandler(resp.GetSeqnum(), resp.GetWhnum(), resp.GetThings())
	}
}

func (h *AmazonWorldHandler) HandlePacked(resp *proto.APacked) {
	if h.packedHandler != nil {
		h.packedHandler(resp.GetSeqnum(), resp.GetShipid())
	} else {
		defaultPackedHandler(resp.GetSeqnum(), resp.GetShipid())
	}
}

func (h *AmazonWorldHandler) HandleLoaded(resp *proto.ALoaded) {
	if h.loadedHandler != nil {
		h.loadedHandler(resp.GetSeqnum(), resp.GetShipid())
	} else {
		defaultLoadedHandler(resp.GetSeqnum(), resp.GetShipid())
	}
}

func (h *AmazonWorldHandler) HandlePackage(resp *proto.APackage) {
	if h.packageHandler != nil {
		h.packageHandler(resp.GetSeqnum(), resp.GetPackageid(), resp.GetStatus())
	} else {
		defaultPackageHandler(resp.GetSeqnum(), resp.GetPackageid(), resp.GetStatus())
	}
}

func (h *AmazonWorldHandler) HandleErr(resp *proto.AErr) {
	if h.errHandler != nil {
		h.errHandler(resp.GetSeqnum(), resp.GetOriginseqnum(), resp.GetErr())
	} else {
		defaultErrHandler(resp.GetSeqnum(), resp.GetOriginseqnum(), resp.GetErr())
	}
}

func defaultPurchaseMoreHandler(seqNum int64, whnum int32, things []*proto.AProduct) {
	slog.Info("Received APurchaseMore response", "seqnum", seqNum, "whnum", whnum, "things", things)
}

func defaultPackedHandler(seqNum int64, shipid int64) {
	slog.Info("Received APacked response", "seqnum", seqNum, "shipid", shipid)
}

func defaultLoadedHandler(seqNum int64, shipid int64) {
	slog.Info("Received ALoaded response", "seqnum", seqNum, "shipid", shipid)
}

func defaultPackageHandler(seqNum int64, packageid int64, status string) {
	slog.Info("Received APackage response", "seqnum", seqNum, "packageid", packageid, "status", status)
}

func defaultErrHandler(seqNum int64, originSeqNum int64, err string) {
	slog.Error("Received error from world simulator", "seqnum", seqNum, "originseqnum", originSeqNum, "error", err)
}
