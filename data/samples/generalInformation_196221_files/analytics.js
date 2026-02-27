(function () {
    if (!window.epzAnalytics) {
        window.epzAnalytics = new function () {
            var self = this;
            self._targetTZ = 'Europe/Moscow';
            self._paramsFilter = [];
            self._cutoff = true;

            /**
             * Sets set of explicit params used to track url partially (e.g. include only object id without page/filtering params)
             * @param params Array of explicit params. Empty array means NO parameters will be included at all. Null will post all params.
             */
            self.setGlobalParamsFilter = function (params, cutoff) {
                self._paramsFilter = params;
                self._cutoff = cutoff;
            };

            /** Posts tracking information to ElasticSearch */
            self.track = function (url, paramsFilter) {
                var defer = $.Deferred();
                var request = new XMLHttpRequest();
                request.open("GET", getServerUrl());
                request.onload = function() {
                    if (this.status == 200) {
                        var serverDate = new Date(this.getResponseHeader("Date"));
                        if (epzCommonConfig && epzCommonConfig.urls.epzEsUrl) {
                            url = prepareUrl(url, paramsFilter);
                            var addr = window.location.origin + epzCommonConfig.urls.epzEsUrl;
                            $.post(addr, JSON.stringify({
                                date: getCurrentDate(serverDate),
                                url: url
                            }), function (data) {
                                defer.resolve();
                                //console.log('Analytics request to "' + url + '" returns response: ' + JSON.stringify(data));
                            }).fail(function () {
                                defer.resolve();
                            });
                        } else {
                            defer.resolve();
                        }
                    } else {
                        defer.resolve();
                        console.log("Analytics can't obtain server time");
                    }
                }
                request.send();
                return defer;
            };

            function getCurrentDate(date) {
                if (moment && moment.tz && moment.tz.zone(self._targetTZ)) {
                    return moment(date).tz(self._targetTZ).format('YYYY-MM-DDTHH:mm:ss.SSSZ');
                }
                return JSON.stringify(date).replace(/"/g,"");
            }

            function prepareUrl(url, paramsFilter) {
                if (!url) {
                    url = window.location.pathname + window.location.search;
                    paramsFilter = self._paramsFilter;
                }
                if (self._cutoff) {
                    url = url.substring(0, url.lastIndexOf('/'));
                }
                if (paramsFilter && URI) {
                    var uri = new URI(url);
                    uri.search(function(data) {
                        var params = {};
                        paramsFilter.forEach(function (e) {
                            if (data[e]) params[e] = data[e];
                        });
                        return params;
                    });
                    url = uri.toString();
                }
                return url;
            }

            function getServerUrl() {
                var URL = contextPath + "/js/common/empty.js";
                return URL + "?noCache=" + Date.now();
            }
        };
    }
})(/*epzCommonConfig*/);