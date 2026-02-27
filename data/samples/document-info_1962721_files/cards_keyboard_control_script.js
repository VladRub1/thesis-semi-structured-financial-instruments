$(document).ready(function () {
    startNavigation = cardStartNavigation;
});


function getPoorVisionSettingsBlock() {
    var resultArray = null;
    if ($.cookie('usePoorVisionOption') == 'true') {
        resultArray = getPoorvisionSettitngsBlock();
        return resultArray;
    }
    resultArray = componentsCollector(".poorVisionSettingsBlock");
    return resultArray;
}
function getCardHeaderElements() {
    var resultArray = componentsCollector(".cardHeader");
    return resultArray;
}

function getContentHeadingWrapper() {
    var resultArray = componentsCollector(".contentHeadingWrapper");
    return resultArray;
}
function getContentTabs() {
    var resultArray = componentsCollector(".contentTabs.noticeTabs");
    return resultArray;
}
function getRevisionToggleWrapper() {
    var resultArray = componentsCollector(".revisionToggleWrapper");
    return resultArray;
}
function getTabBoxBlock() {
    switch (true) {
        case $(".contentTabBoxBlock").length > 0 :
            var container = $(".contentTabBoxBlock").first();
            break;
        case $(".noticeTabBox").length > 0 :
            var container = $(".noticeTabBox");
            break;
        case $(".noticeTabBox-wrapper").length > 0 :
            var container = $(".noticeTabBox-wrapper");
            break;
        default:
            var container = $(".contentTabBoxBlock, .noticeTabBox").first();
    }
    var resultArray = componentsCollector(container);
    return resultArray;
}
function getTableGridBlock() {
    var container = $(".tableGrid");
    var resultArray = componentsCollector(container);
    return resultArray;
}

function getLinkAddInfo() {
    var container = $(".showDetailDocumentInfo");
    var resultArray = componentsCollector(container);
    return resultArray;
}

function getInnerHtml() {
    var container = $(".innerHtml");
    var resultArray = componentsCollector(container);
    return resultArray;
}

function getViewInactiveDocBlock() {
    var container = $(".classSelectorClickAttach");
    var resultArray = componentsCollector(container);
    return resultArray;
}

function getObozStatusBlock() {
    var container = $(".registerSmallBox").closest(".greyBox").children("ul");
    var resultArray = componentsCollector(container);
    container = $(".registerSmallBox");
    resultArray = resultArray.concat(componentsCollector(container));
    return resultArray;
}

function cardStartNavigation() {
    stopNavigation();
    mainNavigationTrack = new FocusesElementsMap();
    var poorvisionControlSectionFunc = function () {
        return getPoorVisionSettingsBlock();
    };

    var cardHeaderSectionFunc = function () {
        return getCardHeaderElements();
    };

    var contentHeadingWrapperSectionFunc = function () {
        return getContentHeadingWrapper();
    };

    var contentTabsFunc = function () {
        return getContentTabs();
    };

    var contentObozStatusBlockFunc = function () {
        return getObozStatusBlock();
    }

    var revisionToggleWrapperFunc = function () {
        return getRevisionToggleWrapper();
    };

    var contentViewInactiveDocSectionFunc = function () {
        return getViewInactiveDocBlock();
    };

    var noticeTabBoxWrapperFunc = function () {
        return getNoticeTabBoxWrapperBlock();
    };

    var tableGridFunc = function () {
        return getTableGridBlock();
    };

    var linkAddInfo = function () {
        return getLinkAddInfo();
    };

    mainNavigationTrack.addSection("poorvisionControlSection", poorvisionControlSectionFunc);
    mainNavigationTrack.addSection("cardHeaderSection", cardHeaderSectionFunc);
    mainNavigationTrack.addSection("contentTabs", contentTabsFunc);
    mainNavigationTrack.addSection("contentHeadingWrapperSection", contentHeadingWrapperSectionFunc);
    mainNavigationTrack.addSection("revisionToggleWrapperSection", revisionToggleWrapperFunc);
    mainNavigationTrack.addSection("viewInactiveDocSection", contentViewInactiveDocSectionFunc);
    mainNavigationTrack.addSection("tableGridSection", tableGridFunc);
    mainNavigationTrack.addSection("contentObozStatusBlockSection", contentObozStatusBlockFunc);
    mainNavigationTrack.addSection("linkAddInfo", linkAddInfo);

    var innerHtml = $("div.innerHtml");
    var tabBoxBlockSectionFunc = function () {
        return getTabBoxBlock();
    };

    if (innerHtml.length > 0) {
        $("div.innerHtml").ajaxStop(function () {
            var isPopUp = $("div.popUp").length > 0;
            if (!isPopUp) {
                mainNavigationTrack.addSection("tabBoxBlockSection", tabBoxBlockSectionFunc);
            }
        });
    }
    var tabBoxBlockSection = getTabBoxBlock();
    mainNavigationTrack.addSection("tabBoxBlockSection", tabBoxBlockSectionFunc);

    var result = mainNavigationTrack.findElementPositionByItem(clicked_element);
    if (result != null) {
        mainNavigationTrack.currentPosition.currentSectionName = result.sectionName;
        mainNavigationTrack.currentPosition.positionInCurrentSection = result.position;
        mainNavigationTrack.next();
    } else {
        mainNavigationTrack.currentPosition.currentSectionName = Object.keys(mainNavigationTrack.navigationBloksMap)[0];
        mainNavigationTrack.currentPosition.positionInCurrentSection = 0;
    }

    mainNavigationTrack.getCurrentElement().setFocus();
    $('.showDetailDocumentInfo').on('click', function () {
        mainNavigationTrack.addSection("tabBoxBlockSection", tabBoxBlockSectionFunc);
    })
}





