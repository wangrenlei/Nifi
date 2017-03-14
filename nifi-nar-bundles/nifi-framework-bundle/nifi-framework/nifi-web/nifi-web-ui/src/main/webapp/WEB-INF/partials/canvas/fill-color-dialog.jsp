<%--
 Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
--%>
<%@ page contentType="text/html" pageEncoding="UTF-8" session="false" %>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt" %>
<div id="fill-color-dialog" class="hidden">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name"><fmt:message key="partials.canvas.fill-color-dialog.Color"/></div>
            <div class="setting-field">
                <input type="text" id="fill-color" value="#FFFFFF"/>
            </div>
            <div class="setting-name" style="margin-top: 10px;"><fmt:message key="partials.canvas.fill-color-dialog.Value"/></div>
            <div class="setting-field">
                <input type="text" id="fill-color-value" value="#FFFFFF"/>
            </div>
            <div class="setting-name" style="margin-top: 10px;"><fmt:message key="partials.canvas.fill-color-dialog.Preview"/></div>
            <div class="setting-field">
                <div id="fill-color-processor-preview" class="icon icon-processor"></div>
                <div id="fill-color-label-preview">
                    <div id="fill-color-label-preview-value"><fmt:message key="partials.canvas.fill-color-dialog.Label"/></div>
                </div>
            </div>
        </div>
    </div>
</div>