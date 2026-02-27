//yandex maps заменены на OpenStreetMap
$(document).ready(function () {
    fixIEConsoleLog();

    const userRegionName = $.cookie("userRegionName"),
          userRegionId = $.cookie("userRegionId"),
          doNotShowKladrPopUp = $.cookie("doNotShowKladrPopUp"),
          doNotAdviseToChangeLocation = $.cookie("doNotAdviseToChangeLocation"),
          doNotAdviseToChangeLocationWhenIosReject = $.cookie("doNotAdviseToChangeLocationWhenIosReject"),
          goodVisionLink = $(document).find('.goodVisionLink');

    goodVisionLink.click(function (e) {
        e.preventDefault();
        $.removeCookie('usePoorVisionOption', { path: '/'});
        $.removeCookie('colorSpectrumForPoorVision', { path: '/'});
        $.removeCookie('fontSizeForPoorVision', { path: '/'});
        location.reload();
    });

    if (userRegionId != null && userRegionId !== 0) {
        setHeadersAndCookiesWhenRegionIdDefined(doNotAdviseToChangeLocation, userRegionName, userRegionId, doNotAdviseToChangeLocationWhenIosReject);
    } else {
        setHeadersAndCookiesWhenRegionIdNotDefined(doNotAdviseToChangeLocation, doNotShowKladrPopUp, userRegionName, doNotAdviseToChangeLocationWhenIosReject);
    }

    $(".poorVisionLink").click(function(e) {
        e.preventDefault();
        $.cookie('usePoorVisionOption', 'true', {path: '/', expires: 365*100});
        location.reload();
    });
});


function setHeadersAndCookiesWhenRegionIdDefined(doNotAdviseToChangeLocation, userRegionName, userRegionId, doNotAdviseToChangeLocationWhenIosReject) {
    Cookies.set('selectElementName', userRegionName);
    setRegionNameHeader(userRegionName);
    if(doNotAdviseToChangeLocation == 'true' || doNotAdviseToChangeLocationWhenIosReject == 'true') {
        return;
    }

    detectGeolocation(function(responseFromOMSGeolocationService) {
        const administrativeAreaName = getAdministrativeAreaName(responseFromOMSGeolocationService),
            subAdministrativeAreaName = getSubAdministrativeAreaName(responseFromOMSGeolocationService);

        const preparedUserRegionName = prepareRegionName(getUserRegionName(userRegionName, administrativeAreaName, subAdministrativeAreaName));

        if(preparedUserRegionName && preparedUserRegionName.trim()) {
            getRegionIdByName(preparedUserRegionName, function(data) {
                const regionId = data ? data.id : null;
                if(regionId && regionId != userRegionId) {
                    $.cookie("possibleRegionIdChange", regionId, { path: '/epz', expires: 1 });
                    $("#popUpUserRegion").click();
                }
            });
        }

    });

}

function setHeadersAndCookiesWhenRegionIdNotDefined(doNotAdviseToChangeLocation, userRegionName, doNotShowKladrPopUp, doNotAdviseToChangeLocationWhenIosReject) {
    setRegionNameHeader("Не выбран");
    if (doNotAdviseToChangeLocation == 'true' || doNotAdviseToChangeLocationWhenIosReject == 'true') {
        return;
    }

    detectGeolocation(function(responseFromOMSGeolocationService) {
        const administrativeAreaName = getAdministrativeAreaName(responseFromOMSGeolocationService),
            subAdministrativeAreaName = getSubAdministrativeAreaName(responseFromOMSGeolocationService);

        userRegionName = prepareRegionName(getUserRegionName(userRegionName, administrativeAreaName, subAdministrativeAreaName));

        if(userRegionName && userRegionName.trim()) {
            getRegionIdByName(userRegionName, function(data) {
                const regionId = data ? data.id : null;
                $.cookie("detectedRegionId", regionId, { path: '/epz', expires: 1 });
            });
        }
        $.cookie("userRegionName", userRegionName, { path: '/epz', expires: 182 });
        $.cookie("yandexRegionName", administrativeAreaName + ', ' + subAdministrativeAreaName, { path: '/epz', expires: 182 });

        if (doNotShowKladrPopUp != 'true') {
            $("#popUpUserRegion").click();
        }
    });
}

function getUserRegionName(userRegionName, administrativeAreaName, subAdministrativeAreaName) {
    if (findFederalRegionAnswer(administrativeAreaName)) {
        userRegionName = subAdministrativeAreaName;
    } else {
        userRegionName = administrativeAreaName;
    }
    return userRegionName;
}

/* fix console.log in IE */
function fixIEConsoleLog() {
    if (!window.console) {
        window.console = {};
    }
    if (!window.console.log) {
        window.console.log = function () { };
    }
}