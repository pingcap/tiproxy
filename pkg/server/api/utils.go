package api

import "net/http"

type CommonJsonResp struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func CreateJsonResp(code int, msg string) CommonJsonResp {
	return CommonJsonResp{
		Code: code,
		Msg:  msg,
	}
}

func CreateSuccessJsonResp() CommonJsonResp {
	return CommonJsonResp{
		Code: http.StatusOK,
		Msg:  "success",
	}
}
