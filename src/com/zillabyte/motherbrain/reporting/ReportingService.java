package com.zillabyte.motherbrain.reporting;

import com.zillabyte.motherbrain.api.APIException;

public interface ReportingService {

	void SendToAPI(String _flowId, String _message, Exception e) throws APIException;
}
