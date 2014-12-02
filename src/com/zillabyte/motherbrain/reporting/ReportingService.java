package com.zillabyte.motherbrain.reporting;

import com.zillabyte.motherbrain.api.APIException;

public interface ReportingService {

	void report(String flowId, String message, Exception e) throws APIException;
}
