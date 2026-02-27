$(document).on("startPopUp", function (event, param) {
    if (param==1) {
        startPopUp(undefined, true);
        return;
    }
    startPopUp(undefined, false);
});
$(document).on("beforeShow", "div.msgBox", function () {
    stopNavigation();
});
function isDisplayNone() {
    return $(this).css("display") == "none";
}
function useCurrentNavigationTrack() {
    if (!(mainNavigationTrack==null||mainNavigationTrack.isEmpty()) && !(mainNavigationTrack.getCurrentElement() instanceof DatePikerNavElement)){
        return false;
    }
    return true;
}
$(window).bind('keydown', function (event) {
    switch (event.keyCode) {
        case 37:    //LEFT
            var currentTrack=getCurrentNavigationTrack();
            if (currentTrack == null) {return;}
            var currentElement = currentTrack.getCurrentElement();
            currentElement.actionLeft(event,currentTrack);
            break;
        case 38:    //UP
            var currentTrack=useCurrentNavigationTrack() ? getCurrentNavigationTrack() : getAnyInnerNavigationTrack();
            if (currentTrack == null ){ return;}
            var currentElement = currentTrack.getCurrentElement();
            currentElement.actionUp(event,currentTrack);
            break;
        case 39:    //RIGHT
            var currentTrack=getCurrentNavigationTrack();
            if (currentTrack == null) {return;}
            var currentElement = currentTrack.getCurrentElement();
            currentElement.actionRighth(event,currentTrack);
            break;
        case 40:    //DOWN
            var currentTrack=useCurrentNavigationTrack() ?getCurrentNavigationTrack() : getAnyInnerNavigationTrack();
            if (currentTrack == null){ return;}
            var currentElement = currentTrack.getCurrentElement();
            currentElement.actionDown(event,currentTrack);
            break;

        case 46:    //DELETE
            var currentTrack=getCurrentNavigationTrack();
            if (currentTrack == null) {return;}
            var currentElement = currentTrack.getCurrentElement();
            currentElement.actionDelete(event,currentTrack);
            break;

        case 27:    //ESCAPE
            if ($(".closePopUp").is(":visible")) {
                $(".closePopUp").click();
            }
            var currentTrack=getCurrentNavigationTrack();
            if (currentTrack == null) {return;}
            var currentElement = currentTrack.getCurrentElement();
            currentElement.actionEscape(event,currentTrack);
            break;

        case 32:    //SPACE
            var currentTrack=getCurrentNavigationTrack();
            if (currentTrack == null) {return;}
            var currentElement = currentTrack.getCurrentElement();
            currentElement.actionSpace(event,currentTrack);
            break;
        case 9:
            if (event.shiftKey) {    //SHIFT + TAB
                var currentTrack = useCurrentNavigationTrack() ? getCurrentNavigationTrack() : mainNavigationTrack;
                if (currentTrack==null||currentTrack.isEmpty()){return;}
                var currentElement = currentTrack.getCurrentElement();
                currentElement.actionShiftTab(event,currentTrack);
            } else {    //TAB
                var currentTrack = useCurrentNavigationTrack() ? getCurrentNavigationTrack() : mainNavigationTrack;
                event.preventDefault();
                if (currentTrack==null||currentTrack.isEmpty()){
                    if ($("div.popUp").length>0  || $("div.msgBox").length>0){
                        startPopUp(undefined, true);
                        return;
                    } else {
                        startNavigation();
                        return;
                    }
                }
                var currentElement = currentTrack.getCurrentElement();
                currentElement.actionTab(event,currentTrack);
            }

            break;

        case 13:    //ENTER
            var currentElement = null;
            var currentTrack=getCurrentNavigationTrack();
            if (currentTrack == null){ return;}
            currentElement=currentTrack.getCurrentElement();
            currentElement.action(event,currentTrack);
            break;
        case 77:
            if (event.ctrlKey || event.metaKey) { //CTRL + M
                event.preventDefault();
                startNavigation();
                mainNavigationTrack.getCurrentElement().loseFocus();
                mainNavigationTrack.currentPosition.currentSectionName = "navigation";
                mainNavigationTrack.currentPosition.positionInCurrentSection = 0;
                mainNavigationTrack.getCurrentElement().setFocus();

            }
            break;
    }
});

function usePoorVisionOption(){
    return $.cookie('usePoorVisionOption') == 'true';
}

function getCurrentNavigationTrack(){
    switch (true) {
        case (lastNavigationTrack!=null):
            return lastNavigationTrack;
            break;
        case (innerNavigationTrack!=null):
            return innerNavigationTrack;
            break;
        case (mainNavigationTrack!=null):
            return mainNavigationTrack;
            break;
        default:
            return null;
    }
}

function getAnyInnerNavigationTrack(){
    switch (true) {
        case (lastNavigationTrack!=null):
            return lastNavigationTrack;
            break;
        case (innerNavigationTrack!=null):
            return innerNavigationTrack;
            break;
        default:
            return null;
    }
}

var mainNavigationTrack = null;
var innerNavigationTrack = null;
var lastNavigationTrack = null;
var clicked_element = null;


function FocusesElementsMap() {
    this.navigationBloksMap = {};//new Map();
    this.blockNameToSelectorMapper = {};

    this.currentPosition = {
        currentSectionName: null,
        positionInCurrentSection: 0
    };


    this.addSection = function (/*sectionName*/ sectionName, collectorFunction) {
        var section = collectorFunction();
        if (section != null && section.length != 0) {
            this.navigationBloksMap[sectionName]=section;
            this.blockNameToSelectorMapper[sectionName]=collectorFunction;
            if (this.currentPosition.currentSectionName == null) {
                this.currentPosition.currentSectionName = sectionName;
            }
        }
    };

    this.getCurrentElement = function () {
        return this.getElementByPosition(this.currentPosition.currentSectionName, this.currentPosition.positionInCurrentSection);
    };

    this.getElementByPosition = function (/*sectionName*/sName, /*positionInSection*/posInSection) {
        return this.navigationBloksMap[sName][posInSection];
    };

    this.getNextElement = function () {
        var nextPositionInSection = 0;
        var nextSectionName = null;
        var sectionsArray = Object.keys(this.navigationBloksMap);
        var _loopedSectionArray = new _loopedArray(sectionsArray, sectionsArray.indexOf(this.currentPosition.currentSectionName));
        var lengthOfCurrentSection = this.navigationBloksMap[this.currentPosition.currentSectionName].length;

        var posInCurSection = this.currentPosition.positionInCurrentSection;
        posInCurSection++;
        if (posInCurSection == lengthOfCurrentSection) {
            nextPositionInSection = 0;
            nextSectionName = _loopedSectionArray.next();
        } else {
            nextPositionInSection = posInCurSection;
            nextSectionName = this.currentPosition.currentSectionName;
        }
        var result = {
            nextSectionName: nextSectionName,
            nextPositionInSection: nextPositionInSection,
            element: this.getElementByPosition(nextSectionName, nextPositionInSection)
        };
        return result;

    };

    this.getPrevElement = function () {
        var nextPositionInSection = 0;
        var nextSectionName = null;
        var sectionsArray = Object.keys(this.navigationBloksMap);
        var _loopedSectionArray = new _loopedArray(sectionsArray, sectionsArray.indexOf(this.currentPosition.currentSectionName));

        var posInCurSection = this.currentPosition.positionInCurrentSection;
        posInCurSection--;
        if (posInCurSection == -1) {
            nextSectionName = _loopedSectionArray.prev();
            nextPositionInSection = this.navigationBloksMap[nextSectionName].length - 1;
        } else {
            nextPositionInSection = posInCurSection;
            nextSectionName = this.currentPosition.currentSectionName;
        }
        var result = {
            nextSectionName: nextSectionName,
            nextPositionInSection: nextPositionInSection,
            element: this.getElementByPosition(nextSectionName, nextPositionInSection)
        };
        return result;
    };

    this.getPrevOrNextCurrentSectionElementVerticalNavigation = function (columns, step) {
        var posInCurSection = this.currentPosition.positionInCurrentSection;
        var length = this.navigationBloksMap[this.currentPosition.currentSectionName].length;
        var nextPosition = posInCurSection + step * columns;
        if (nextPosition > length - 1) {
            nextPosition = posInCurSection % columns;
        } else if (nextPosition < 0){
            nextPosition = length - 1 - (length - 1 - posInCurSection) % 7;
        }
        this.currentPosition.positionInCurrentSection = nextPosition;
        return this.getCurrentElement();
    };

    this.getPrevOrNextCurrentSectionElement = function (step) {
        var posInCurSection = this.currentPosition.positionInCurrentSection;
        var length = this.navigationBloksMap[this.currentPosition.currentSectionName].length
        var nextPosition = posInCurSection + step;
        if (nextPosition > length - 1) {
            nextPosition = 0;
        } else if (nextPosition < 0){
            nextPosition = length - 1;
        }
        this.currentPosition.positionInCurrentSection = nextPosition;
        return this.getCurrentElement();
    };

    this.sectionLastElement = function (section) {
        if (this.navigationBloksMap[section] != undefined && this.navigationBloksMap[section].length > 0){
            this.currentPosition.currentSectionName = section;
            this.currentPosition.positionInCurrentSection = this.navigationBloksMap[section].length-1;
        }
        return this.getCurrentElement();
    };

    this.sectionFirstElement = function (section) {
        if (this.navigationBloksMap[section] != undefined && this.navigationBloksMap[section].length > 0){
            this.currentPosition.currentSectionName = section;
            this.currentPosition.positionInCurrentSection = 0;
        }
        return this.getCurrentElement();
    };

    this.next = function () {
        var nextElement = this.getNextElement();
        this.currentPosition.currentSectionName = nextElement.nextSectionName;
        this.currentPosition.positionInCurrentSection = nextElement.nextPositionInSection;
        return this.getCurrentElement();
    };
    this.prev = function () {
        var prevElent = this.getPrevElement();
        this.currentPosition.currentSectionName = prevElent.nextSectionName;
        this.currentPosition.positionInCurrentSection = prevElent.nextPositionInSection;
        return this.getCurrentElement();
    };

    function _loopedArray(/*array*/arr, /*position*/position) {
        this.arr = arr;
        this.currentPosition = position;

        this.next = function () {
            this.currentPosition++;
            if (this.currentPosition == this.arr.length) {
                this.currentPosition = 0;
            }
            return this.arr[this.currentPosition];

        };

        this.prev = function () {
            this.currentPosition--;
            if (this.currentPosition == -1) {
                this.currentPosition = this.arr.length - 1;
            }
            return this.arr[this.currentPosition];

        }

    }

    /*not tested yet*/
    this.removeSectionByName = function (/*name*/name) {
        var hasSuchSection = name in this.navigationBloksMap;
        if (!hasSuchSection) {
            return;
        } else {
            if (name === this.currentPosition.currentSectionName) {
                var sectionsArray = Object.keys(this.navigationBloksMap);
                var _loopedSectionArray = new _loopedArray(sectionsArray, sectionsArray.indexOf(this.currentPosition.currentSectionName));
                var nextSectionName = _loopedSectionArray.next();
                this.currentPosition.currentSectionName = nextSectionName;
                this.currentPosition.positionInCurrentSection = 0;

            }
            delete this.navigationBloksMap[name];
        }
    };

    this.findElementPositionInSection = function (sectionName, element) {
        return _arrayObjectIndexOf(this.navigationBloksMap[sectionName], element.item, "item");
    };

    function _arrayObjectIndexOf(myArray, searchTerm, property) {
        for (var i = 0, len = myArray.length; i < len; i++) {
            if (myArray[i][property] === searchTerm) return i;
        }
        return -1;
    }

    this.findElementPositionInAllSections = function (element) {
        var sectionsArray = Object.keys(this.navigationBloksMap);
        sectionsArray.forEach(function (item) {
                var result = this.findElementPositionInSection(item, element);
                if (result != null) {
                    return result = {section: item, position: result};
                }
            }
        );
        return null;
    };

    this.isEmpty = function(){
        return Object.keys(this.navigationBloksMap).length==0;
    };

    this.findElementPositionByItem=function(item){
        if (item==null) return null;
        for (var property in this.navigationBloksMap) {
            if (this.navigationBloksMap.hasOwnProperty(property)) {
                var arr=this.navigationBloksMap[property];
                for (var i = 0, len = arr.length; i < len; i++) {
                    var it = arr[i].item;
                    if (it && ($(it).is($(item)) ||
                        $.contains(it, $(item)[0]))) {
                        return {
                            element: arr[i],
                            sectionName: property,
                            position: i
                        };
                    }

                }
            }

        }
        return null;
    };
    this.getCurrentSectionLength=function(){
        return this.navigationBloksMap[this.currentPosition.currentSectionName].length;
    }

}


function startNavigation() {
    stopNavigation();
    mainNavigationTrack = new FocusesElementsMap();

    var _navMenuFunc = function(){
        var _components=componentsMenuCollecor();
        return _components.navMenuArray;
    };
    var _headerArrayFunc = function(){
        return getHeaderElementsNew();
    };
    var _searchFormArrayFunc =function(){
        return sortSection(getSearchFormElements(), "searchFormArray");
    };
    var _footerArrayFunc =function(){
        return getFooterElementsNew();
    };
    var _resultSearchFunc =function(){
        return getSearchResultElements();
    };
    var _btnUpFunc =function(){
        return getBtnUpElement();
    };

    var _sortByArrayFunc =function(){
        return sortSection(getSortByElements(), "sortBy");
    };

    var _topPaginatorFunc=function(){
        return getPaginatorElements(".paginator.greyBox:first");
    };

    var _listRowBlockFunc=function(){
        return getListRowBlockElements("#listRowBlock");
    };

    var _footerPaginatorFunc=function(){
        return getPaginatorElements(".paginator.greyBox:last");
    };


    if ($.cookie('usePoorVisionOption') == 'true') {
        var _poorvisionControlsArrayFunc = function(){
            return getPoorvisionSettitngsBlock();
        };
        mainNavigationTrack.addSection("poorControls", _poorvisionControlsArrayFunc);
    }
    mainNavigationTrack.addSection("header", _headerArrayFunc);
    mainNavigationTrack.addSection("navigation", _navMenuFunc);
    mainNavigationTrack.addSection("searchForm", _searchFormArrayFunc);
    mainNavigationTrack.addSection("sortBy", _sortByArrayFunc);
    mainNavigationTrack.addSection("topPaginator", _topPaginatorFunc);
    mainNavigationTrack.addSection("resultSearch", _resultSearchFunc);
    mainNavigationTrack.addSection("listRowBlock", _listRowBlockFunc);
    mainNavigationTrack.addSection("footerPaginator", _footerPaginatorFunc);
    mainNavigationTrack.addSection("btnUp", _btnUpFunc);
    mainNavigationTrack.addSection("footer", _footerArrayFunc);

    var result=mainNavigationTrack.findElementPositionByItem(clicked_element);
    if (result != null){
        mainNavigationTrack.currentPosition.currentSectionName = result.sectionName;
        mainNavigationTrack.currentPosition.positionInCurrentSection = result.position;
        mainNavigationTrack.next();
    } else {
        mainNavigationTrack.currentPosition.currentSectionName = Object.keys(mainNavigationTrack.navigationBloksMap)[0];
        mainNavigationTrack.currentPosition.positionInCurrentSection = 0;
    }

    mainNavigationTrack.getCurrentElement().setFocus();
}

function AbstractNavElement(/*element*/ item) {
    this.item = item;

    this.setFocus = function () {
        this.setStyleForElement(this.item);
        //scrollToElelment(this.item);
        var isPopUp = $("div.popUp").length > 0;
        if (isPopUp){
            scrollToRelativeElement(this.item,$("div.popUpWrapper"));
        } else {
            scrollToElelment(this.item);
        }

        var isMenuElement = this instanceof AbstractMenuElement;
        if (!menuIsCollapsed && !isMenuElement) {
            collapseNavMenu();
        }
    };

    this.loseFocus = function () {
        this.removeStyleFromElement(this.item);
    };

    this.action = function (event,track) {
        event.preventDefault();
        $(item).trigger('click',[1]);
    };

    this.actionDelete = function (event,track) {

    };

    this.actionSpace = function (event,track) {

    };

    this.actionDown=function(event,track){
        event.preventDefault();
        this.loseFocus();
        var prevElement = track.next();
        prevElement.setFocus();
    };
    this.actionUp=function(event,track){
        event.preventDefault();
        this.loseFocus();
        var prevElement = track.prev();
        prevElement.setFocus();
    };

    this.actionLeft = function (event,track) {

    };

    this.actionRighth = function (event,track) {

    };

    this.actionEscape = function (event,track) {

    };

    this.actionTab = function (event,track){
        event.preventDefault();
        this.loseFocus();
        var nextElement = track.next();
        nextElement.setFocus(true);

    };

    this.actionShiftTab = function (event,track){
        event.preventDefault();
        this.loseFocus();
        var prevElement = track.prev();
        prevElement.setFocus(false);
    };

    this.setStyleForElement = function () {
        $(this.item).addClass("outlineRedKeyboardControl");
    };

    this.removeStyleFromElement = function () {
        $(this.item).removeClass("outlineRedKeyboardControl");
    };

}

function ViewInactiveDocElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this, event, track);
        var section = 'tabBoxBlockSection';
        var func=mainNavigationTrack.blockNameToSelectorMapper[section];
        if (func != undefined) {
            mainNavigationTrack.addSection(section, func);
        }
    }

    this.setStyleForElement = function () {
        $(this.item).parent(".classSelectorClickAttach").addClass("outlineRedKeyboardControl");
    };

    this.removeStyleFromElement = function () {
        $(this.item).parent(".classSelectorClickAttach").removeClass("outlineRedKeyboardControl");
    };
}

function SortItemElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    this.setDefaultStyleForElement = function () {
        $(this.item)
            .css("margin-right", "8px")
            .css("padding-right", "0px");
    };
    if (!usePoorVisionOption()) {
        this.setDefaultStyleForElement();
    }

    this.setStyleForElement = function () {
        $(this.item).children("span").addClass("outlineRedKeyboardControl");
    };
    this.removeStyleFromElement = function () {
        $(this.item).children("span").removeClass("outlineRedKeyboardControl");
    };
}

function TextNavElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    this.action = function (event,track) {

    };

    this.actionDelete = function (event,track) {

    };

    this.actionSpace = function (event,track) {

    };

    this.actionUp = function (event,track) {

    };

    this.actionDown = function (event,track) {

    };

    this.actionLeft = function (event,track) {

    };

    this.actionRighth = function (event,track) {

    };

    var _parentSetFocus = this.setFocus;
    this.setFocus = function () {
        _parentSetFocus.call(this);
        $(this.item).focus();

    };

    var _loseSetFocus = this.loseFocus;
    this.loseFocus = function () {
        _loseSetFocus.call(this);
        $(this.item).blur();
    };

    var _parentSetStyleForElement = this.setStyleForElement;
    this.setStyleForElement = function () {
        if (usePoorVisionOption() && $(this.item).parent().is("div.searchField")) {
            $(this.item).parent().addClass("outlineRedKeyboardControl");
            return;
        }
        _parentSetStyleForElement.call(this);
    };

    var _parentRemoveStyleFromElement = this.removeStyleFromElement;
    this.removeStyleFromElement = function () {
        if (usePoorVisionOption() && $(this.item).parent().is("div.searchField")) {
            $(this.item).parent().removeClass("outlineRedKeyboardControl");
            return;
        }
        _parentRemoveStyleFromElement.call(this);
    };
}

function PopUpTextInputElement(/*element*/ item) {
    TextNavElement.apply(this, arguments);

    this.loseFocus = function () {
        this.removeStyleFromElement(this.item);
    };
}

function TextInputWithValidationElement(/*element*/ item) {
    TextNavElement.apply(this, arguments);

    this.loseFocus = function () {
        this.removeStyleFromElement(this.item);

        if ($(this.item).prop("value") != "" || $(this.item).hasClass("ui-autocomplete-input")) {
            $(this.item).blur();
        }
    };
}


function LinkElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {
        event.preventDefault();
        var url = $(item).attr("href");
        if (url == '#' || url == 'javascript:void(0)' || url === undefined) {
            _parentAction.call(this, event, track);
            return;
        }
        if (url != null) {
            window.location.href = url;
        }
    }
}

function LinkParentElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {
        event.preventDefault();
        var url = $(item).parent().attr("href");
        if (url == '#') {
            _parentAction.call(this,event,track);
            return;
        }
        if (url != null) {
            if ( $(item).parent().attr('target') == '_blank')  {
                window.open(url,'_blank');
            } else {
                window.location.href = url;
            }
        }
    }
}

function BtnUpElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {
        event.preventDefault();
        mainNavigationTrack.getCurrentElement().loseFocus();
        if ($.cookie('usePoorVisionOption') != 'true') {
            mainNavigationTrack.currentPosition.currentSectionName = Object.keys(mainNavigationTrack.navigationBloksMap)[0];
        } else {
            mainNavigationTrack.currentPosition.currentSectionName = Object.keys(mainNavigationTrack.navigationBloksMap)[1];
        }
        mainNavigationTrack.currentPosition.positionInCurrentSection = 0;
        mainNavigationTrack.getCurrentElement().setFocus();
    }

    if ($.cookie('usePoorVisionOption') != 'true') {
        this.setStyleForElement = function () {
            $(this.item).addClass("outlineRedBtnUpKeyboardControl");
        };

        this.removeStyleFromElement = function () {
            $(this.item).removeClass("outlineRedBtnUpKeyboardControl");
        };
    }
}

function LinkElementWithDefaultStyle(/*element*/ item) {
    LinkElement.apply(this, arguments);

    this.setDefaultStyleForElement = function () {
        switch (true) {
            case $(this.item).is("a.clientIco"):
                $(this.item)
                    .css("padding-right", "0px")
                    .css("margin-right", "20px");
                break;
            case $(this.item).is("a.rssBox"):
                $(this.item)
                    .css("padding", "0px")
                    .css("margin-right", "5px")
                    .css("margin-bottom", "6px");
                break;
            default:
                return;
        }
    }
    if (!usePoorVisionOption()) {
        this.setDefaultStyleForElement();
    }
}


function ShowListInfoElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);
    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
        $(":animated").promise().done(function () { //wait until all animaitons finished
            var curSectionName = mainNavigationTrack.currentPosition.currentSectionName;
            var currElement = mainNavigationTrack.getCurrentElement();

            var isPopUp = $("div.popUp").length > 0;
            var reloadedSectionElemnets = null;

            if (isPopUp) {
                reloadedSectionElemnets = function () {
                    return componentsCollector("div.popUp");
                };
            } else {
                var func=mainNavigationTrack.blockNameToSelectorMapper[curSectionName];
                reloadedSectionElemnets = func;
            }
            mainNavigationTrack.addSection(curSectionName, reloadedSectionElemnets);

            var newCurEl = mainNavigationTrack.findElementPositionInSection(curSectionName, currElement);
            mainNavigationTrack.currentPosition.currentSectionName = curSectionName;
            mainNavigationTrack.currentPosition.positionInCurrentSection = newCurEl;
        });
    }
}

function LinkLKElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {
        event.preventDefault();
        var url = $(this.item).children();
        if (url == '#') {
            _parentAction.call(this,event,track);
            return;
        }
        if (url != null) {
            if (url.hasClass("linkPopUp")) {
                url.trigger('click',[1]);
            } else {
                var urlTargetBlank = url.attr("href");
                window.open(urlTargetBlank, '_blank');
            }
        }
    };

    this.setStyleForElement = function () {
        $(this.item).addClass("outlineRedKeyboardControl");
    };

    this.removeStyleFromElement = function () {
        $(this.item).removeClass("outlineRedKeyboardControl");
    };
}

function ChooseShowCountElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
    };

    this.setStyleForElement = function () {
        $(this.item).addClass("outlineRedKeyboardControl");
    };

    this.removeStyleFromElement = function () {
        $(this.item).removeClass("outlineRedKeyboardControl");
    };
}

function AbstractMenuElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    this.action = function () {
        //do nothing
    };

    this.setFocus = function () {
        this.setStyleForElement(this.item);
        if (menuIsCollapsed) {
            var navigationMenu = $(this.item).parent().closest('div.navigation');
            expandNavMenu(navigationMenu);
        }
    }
}
AbstractMenuElement.prototype = Object.create(AbstractNavElement.prototype);

function OneLineMenuElement() {
    AbstractMenuElement.apply(this, arguments);

    this.action = function (event,track) {
        event.preventDefault();
        if (($(this.item).parent().next(".nextLevel").length > 0)) {
            $(this.item).children("span").trigger('click',[1]);
            var subMenu = $(this.item).parent().next(".nextLevel").children("li");
            var arr = subMenu.toArray();
            var result = [];
            arr.forEach(function (item) {
                var el = new SecondLevelMenuElement(item);
                result.push(el);
            });
            var func=function(){
              return result;
            };
            innerNavigationTrack = new FocusesElementsMap();
            innerNavigationTrack.addSection("singleTrack", func);
            innerNavigationTrack.getCurrentElement().setFocus();
        } else {
            var url = $(this.item).find("*[href]:first").attr("href");
            window.location.href = url;
        }

    };

    var _parentLoseFocus = this.loseFocus;
    this.loseFocus = function () {
        _parentLoseFocus.call(this);
        if (innerNavigationTrack != null && "singleTrack" in innerNavigationTrack.navigationBloksMap) {
            var curInnerElement = innerNavigationTrack.getCurrentElement();
            curInnerElement.loseFocus();
            innerNavigationTrack = null;
            $(this.item).parents("ul.topLevel").siblings("ul.topLevel").find("li").removeClass("expandStatus");
            $(this.item).parents("ul.topLevel").siblings("ul.nextLevel").slideUp(300);
            $(this.item).removeClass("expandStatus");
        }
    };

    this.setStyleForElement = function () {
        $(this.item).addClass("backgroundColorRedKeyboardControl");
        $(this.item).addClass("outlineRedKeyboardControl");
    };

    this.removeStyleFromElement = function () {
        $(this.item).removeClass("backgroundColorRedKeyboardControl");
        $(this.item).removeClass("outlineRedKeyboardControl");
    };

}
OneLineMenuElement.prototype = Object.create(AbstractMenuElement.prototype);

function SecondLevelMenuElement() {
    AbstractMenuElement.apply(this, arguments);

    this.action = function (event,track) {
        event.preventDefault();
        if ($(this.item).is(":not(.itemLastLevel)")) {
            var url = $(this.item).find("*[href]:first").attr("href");
            window.location.href = url;
        } else {
            $(this.item).addClass("expandStatus").find("ul.lastLevel").slideDown(300);
            var subMenu = $(this.item).find(".lastLevel li");
            var arr = subMenu.toArray();
            var result = new Array();
            arr.forEach(function (item) {
                var el = new LastLevelMenuElement(item);
                result.push(el);
            });
            var func = function (){
              return result;
            };
            lastNavigationTrack = new FocusesElementsMap();
            lastNavigationTrack.addSection("singleTrack", func);
            lastNavigationTrack.getCurrentElement().setFocus();
        }
    };

    var _parentLoseFocus = this.loseFocus;
    this.loseFocus = function () {
        _parentLoseFocus.call(this);
        $(this.item).removeClass("expandStatus").find("ul.lastLevel").slideUp(300);

        if (lastNavigationTrack != null && "singleTrack" in lastNavigationTrack.navigationBloksMap) {
            var curInnerElement = lastNavigationTrack.getCurrentElement();
            curInnerElement.loseFocus();
            lastNavigationTrack = null;
        }
    };

    this.setStyleForElement = function () {
        $(this.item).addClass("outlineRedKeyboardControl")
    };

    this.removeStyleFromElement = function () {
        $(this.item).removeClass("outlineRedKeyboardControl")
    };

    this.actionShiftTab=function (event,track) {
        //do nothing
    };
    this.actionTab=function (event,track) {
        //do nothing
    }
}

SecondLevelMenuElement.prototype = Object.create(AbstractMenuElement.prototype);


function LastLevelMenuElement() {
    AbstractMenuElement.apply(this, arguments);

    this.action = function (event,track) {
        event.preventDefault();
        var url = $(this.item).find("*[href]:first").attr("href");
        window.location.href = url;
    };

    this.actionLeft = function (event,track) {
        event.preventDefault();
        $(this.item).closest(".itemLastLevel").removeClass("expandStatus").find("ul.lastLevel").slideUp(300);

        if (lastNavigationTrack != null && "singleTrack" in lastNavigationTrack.navigationBloksMap) {
            var curInnerElement = lastNavigationTrack.getCurrentElement();
            curInnerElement.loseFocus();
            lastNavigationTrack = null;
        }
        this.loseFocus();
    };

    this.setStyleForElement = function () {
        $(this.item).addClass("outlineRedKeyboardControl")
    };

    this.removeStyleFromElement = function () {
        $(this.item).removeClass("outlineRedKeyboardControl")
    }
}

LastLevelMenuElement.prototype = Object.create(AbstractMenuElement.prototype);


function DeletableNavElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    this.action = function (event,track) {
        //do nothing
    };
    this.actionDelete = function (event,track) {
        event.preventDefault();
        $(this.item).trigger('click',[1]);
    };

    var _parentSetFocus = this.setFocus;
    this.setFocus = function () {
        _parentSetFocus.call(this);
        var isPopUp = $("div.popUp").length > 0;
        if (isPopUp){
            scrollToRelativeElement($(this.item).parent()[0],$("div.popUpWrapper"));
        } else {
            scrollToElelment($(this.item).parent()[0]);
        }
    }

    this.setStyleForElement = function () {
        $(this.item).parent().addClass("outlineRedKeyboardControl")
    };

    this.removeStyleFromElement = function () {
        $(this.item).parent().removeClass("outlineRedKeyboardControl")
    };
}


function CheckBoxNavElement(/*element*/ item) {
    ReloadedNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {
        //do nothing
    };

    this.actionSpace = function (event,track) {
        event.preventDefault();
        _parentAction.call(this,event,track);
    };

    this.setStyleForElement = function () {
        if ($.cookie('usePoorVisionOption') == 'true'){
            $(this.item).next("label").addClass("outlineRedKeyboardControl");
        } else{
            $(this.item).addClass("outlineRedKeyboardControl");
        }
    };

    this.removeStyleFromElement = function () {
        if ($.cookie('usePoorVisionOption') == 'true'){
            $(this.item).next("label").removeClass("outlineRedKeyboardControl");
        } else{
            $(this.item).removeClass("outlineRedKeyboardControl");
        }
    };
}

function SearchAttachedFileTagCheckBoxNavElement(/*element*/ item) {
    ReloadedNavElement.apply(this, arguments);
    this.flag = $(this.item).prop('checked');

    var _parentAction = this.action;
    this.action = function (event,track) {
        //do nothing
    };

    this.actionSpace = function (event, track) {
        event.preventDefault();
        if (this.flag == false) {
            $(this.item).trigger('click',[1]);
            $(this.item).trigger('click',[1]);
            $(this.item).prop('checked', true);
            this.flag = true;
        } else {
            $(this.item).trigger('click',[1]);
            $(this.item).trigger('click',[1]);
            $(this.item).prop('checked', false);
            this.flag = false;
        }
        this.reloadSection();
    };

    this.setStyleForElement = function () {
        if (usePoorVisionOption()){
            $(this.item).next("label").addClass("borderColorRedKeyboardControl");
        } else{
            $(this.item).addClass("outlineRedKeyboardControl");
        }
    };

    this.removeStyleFromElement = function () {
        if (usePoorVisionOption()){
            $(this.item).next("label").removeClass("borderColorRedKeyboardControl");
        } else{
            $(this.item).removeClass("outlineRedKeyboardControl");
        }
    };
}

function PurchasePlansTypesLiCheckBoxNavElement(/*element*/ item) {
    ReloadedNavElement.apply(this, arguments);
    this.flag = false;

    var _parentAction = this.action;
    this.action = function (event,track) {
        //do nothing
    };

    this.actionSpace = function (event, track) {
        event.preventDefault();
        if (this.flag == false) {
            $(this.item).click();
            $(this.item).click();
            $(this.item).prop('checked', false);
            this.flag = true;
        } else {
            $(this.item).click();
            $(this.item).click();

            $(this.item).prop('checked', true);
            this.flag = false;
        }
    };

    this.setStyleForElement = function () {
        if ($.cookie('usePoorVisionOption') == 'true'){
            $(this.item).next("label").addClass("outlineRedKeyboardControl");
        } else{
            $(this.item).addClass("outlineRedKeyboardControl");
        }
    };

    this.removeStyleFromElement = function () {
        if ($.cookie('usePoorVisionOption') == 'true'){
            $(this.item).next("label").removeClass("outlineRedKeyboardControl");
        } else{
            $(this.item).removeClass("outlineRedKeyboardControl");
        }
    };
}

function PopUpCheckBoxNavElement(/*element*/ item) {
    ReloadedNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {

    };

    this.setFocus=function(){
        this.setStyleForElement(this.item);
        var container = null;
        var element = null;
        if ($.cookie('usePoorVisionOption') == 'true'){
            container = $("div.popUpWrapper.overflowX ");
            element = $(this.item).closest("td");
        } else {
            container = $(".overflowX.maxHeight230");
            element = this.item;
        }


        scrollToRelativeElement(element,container);

    };

    this.loseFocus=function(){
        this.removeStyleFromElement(this.item);
    };

    this.actionSpace = function (event,track) {
        event.preventDefault();
        var _isChecked=$(this.item).prop('checked');
        $(this.item).prop('checked', !_isChecked);
        _parentAction.call(this,event,track);
        $(this.item).prop('checked', !_isChecked);
    };
}

function ManySelectorCheckBoxNavElement(/*element*/ item) {
    CheckBoxNavElement.apply(this, arguments);

    this.setFocus=function(){
        this.setStyleForElement(this.item);
        var container = $(this.item).closest(".expanded");
        var element = $(this.item).closest("li");
        scrollToRelativeElement(element,container);
    };

    this.loseFocus=function(){
        this.removeStyleFromElement(this.item);
    };
}

function DynatreeExpanderElement(/*element*/ item){
    AbstractNavElement.apply(this, arguments);

    this.action=function(){
        var expander=$(this.item).siblings(".dynatree-expander ");
        var tree=$("#tree").dynatree("getRoot");
        $("#tree").dynatree({ onExpand: function(flag, node) {
            setElement();
        }});

        $(expander).trigger('click',[1]);
        var currentNode=getNodeObject($(this.item).text(),"#tree");

        $("#tree").ajaxComplete(function() {
            setElement();
        });
    };

    function setElement(){
                var curSectionName = mainNavigationTrack.currentPosition.currentSectionName;
                var curPosition = mainNavigationTrack.currentPosition.positionInCurrentSection;
                var currElement = mainNavigationTrack.getCurrentElement();
                var reloadedSectionElemnetsFunc = function (){return componentsCollector("div.popUp");};
               // var title=this.item

                mainNavigationTrack.addSection(curSectionName, reloadedSectionElemnetsFunc);

                //var newCurEl = mainNavigationTrack.findElementPositionInSection(curSectionName, currElement);
                mainNavigationTrack.currentPosition.currentSectionName = curSectionName;
                mainNavigationTrack.currentPosition.positionInCurrentSection = curPosition;
                mainNavigationTrack.getCurrentElement().setFocus();
    }


    this.setFocus=function(){
        this.setStyleForElement(this.item);
        var container = $(this.item).closest(".dynatree-container");
        scrollToRelativeElement($(this.item).parent().get(0), container);
    };

    this.loseFocus=function(){
        this.removeStyleFromElement(this.item);
    };
    this.setStyleForElement = function () {
        $(this.item).parent().addClass("backgroundColorRedKeyboardControl");
    };

    this.removeStyleFromElement = function () {
        $(this.item).parent().removeClass("backgroundColorRedKeyboardControl");
    };
}

function getNodeObject(title, treeSelector){
    var tree=$(treeSelector).dynatree("getRoot");
    var result = $.grep(tree.childList, function(item){ return item.data.title == title; });
    return result[0];
}

function DynatreeCheckboxElement(/*element*/ item){
    ReloadedNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {

    }

    this.actionSpace = function (event,track) {
        _parentAction.call(this,event,track);
    };

    this.setStyleForElement = function () {
        $(this.item).css("outline","2px solid #e55b44");
    };

    this.removeStyleFromElement = function () {
        $(this.item).css("outline","");
    };

    this.setFocus=function(){
        this.setStyleForElement(this.item);
        var treeExist = ($(this.item).closest(".dynatree-container").length > 0);
        var container = treeExist ? ".dynatree-container" : ".treetable";
        scrollToRelativeElement($(this.item).parent().get(0), $(this.item).closest(container));
    };

    this.loseFocus=function(){
        this.removeStyleFromElement(this.item);
    };

}



function SelectBoxElement(/*element*/ item){
    AbstractNavElement.apply(this, arguments);

    this.action = function (event,track) {
    //do nothing
    };

    var _parentSetFocus=this.setFocus;
    this.setFocus=function(){
        _parentSetFocus.call(this);
        $(this.item).focus();
    };

    var _parentLoseFocus=this.loseFocus;
    this.loseFocus=function(){
        _parentLoseFocus.call(this);
        $(this.item).blur();
    };

    this.setStyleForElement = function () {
        $(this.item).next().addClass("outlineRedKeyboardControl")
    };

    this.removeStyleFromElement = function () {
        $(this.item).next().removeClass("outlineRedKeyboardControl")
    }

}



function PseudoSelectorNavElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    this.actionTab=function () {
        //do nothing
    };
    this.actionShiftTab=function () {
        //do nothing
    };

    this.setFocus=function(){
        this.setStyleForElement(this.item);
        var container = $(this.item).closest(".tuningQuickSearchList");
        scrollToRelativeElement(this.item,container);
    };

    this.loseFocus=function(){
        this.removeStyleFromElement(this.item);
    };
}

function PseudoSelectorControlNavElement(/*element*/ item) {
    PseudoSelectorNavElement.apply(this, arguments);

    this.setStyleForElement = function () {
        $(this.item).addClass("outlineRedKeyboardControlPseudo");
    };

    this.removeStyleFromElement = function () {
        $(this.item).removeClass("outlineRedKeyboardControlPseudo");
    };
}


function ManySelectBtnElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);
    var self = this;
    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
        $(":animated").promise().done(function () { //wait until all animaitons finished
            var curSectionName = mainNavigationTrack.currentPosition.currentSectionName;
            var reloadedSectionElemnetsFunc = function (){ return sortSection(getSearchFormElements(), "searchFormArray"); };

            mainNavigationTrack.addSection(curSectionName, reloadedSectionElemnetsFunc);
            mainNavigationTrack.getCurrentElement().setFocus();
            self.removeStyleFromElement();
        });
    };
}

function CheckBoxNavElementCustomCheckboxSearchField(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
    };

    this.actionSpace = function (event,track) {
        event.preventDefault();
        $(this.item).parent().prop('checked', true);
        this.action.call(this,event,track);
    };

    this.setStyleForElement = function () {
        if ($.cookie('usePoorVisionOption') !== 'true') {
            if ($(this.item).children() !== []) {
                $(this.item).children("input").addClass("outlineRedKeyboardControl");
            } else {
                $(this.item).addClass("outlineRedKeyboardControl");
            }
        } else {
            $(this.item).children("label").addClass("outlineRedKeyboardControlSearch")
        }
    };

    this.removeStyleFromElement = function () {
        if ($.cookie('usePoorVisionOption') !== 'true') {
            if ($(this.item).children() !== []) {
                $(this.item).children("input").removeClass("outlineRedKeyboardControl");
            } else {
                $(this.item).removeClass("outlineRedKeyboardControl");
            }
        } else {
            $(this.item).children("label").removeClass("outlineRedKeyboardControlSearch")
        }
    };
}
function ExpandInfoElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);
     this.flag = false;

    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
    };

    this.actionSpace = function (event, track) {
        event.preventDefault();

        if (this.flag == false) {
            $(this.item).click();
            $(this.item).click();
            $(this.item).prop('checked', true);
            this.flag = true;
            mainNavigationTrack.getCurrentElement().setFocus();
        } else {
            $(this.item).click();
            $(this.item).click();
            $(this.item).prop('checked', false);
            this.flag = false;
            mainNavigationTrack.getCurrentElement().setFocus();
        }

        $(":animated").promise().done(function () { //wait until all animaitons finished
            var reloadedSectionElemnets = mainNavigationTrack.blockNameToSelectorMapper["resultSearch"];
            mainNavigationTrack.addSection("resultSearch", reloadedSectionElemnets);
        });
    };

    this.setStyleForElement = function () {
        if ($.cookie('usePoorVisionOption') != 'true') {
            $(this.item).addClass("outlineRedKeyboardControl");
        } else {
            $(this.item).children("label").addClass("borderColorRedKeyboardControl")
        }
    };

    this.removeStyleFromElement = function () {
        if ($.cookie('usePoorVisionOption') != 'true') {
            $(this.item).removeClass("outlineRedKeyboardControl");
        } else {
            $(this.item).children("label").removeClass("borderColorRedKeyboardControl")
        }
    };
}
function ShowRulesElement(/*element*/ item) {
    console.log("here!")
    AbstractNavElement.apply(this, arguments);
     this.flag = false;

    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
    };

    this.actionSpace = function (event, track) {
        event.preventDefault();

        if (this.flag == false) {
            $(this.item).click();
            $(this.item).click();
            $(this.item).prop('checked', false);
            this.flag = true;
            mainNavigationTrack.getCurrentElement().setFocus();
        } else {
            $(this.item).click();
            $(this.item).click();
            $(this.item).prop('checked', true);
            this.flag = false;
            mainNavigationTrack.getCurrentElement().setFocus();
        }

        $(":animated").promise().done(function () { //wait until all animaitons finished
            var reloadedSectionElemnets = mainNavigationTrack.blockNameToSelectorMapper["resultSearch"];
            mainNavigationTrack.addSection("resultSearch", reloadedSectionElemnets);
        });
    };

    this.setStyleForElement = function () {
        $(this.item).addClass("outlineRedKeyboardControl")
    };

    this.removeStyleFromElement = function () {

            $(this.item).removeClass("outlineRedKeyboardControl")
    };
}
function CheckBoxNavElementCustomCheckSearchField(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    this.actionSpace = function (event,track) {
        event.preventDefault();
        $(this.item).parent().prop('checked', true);
        this.action.call(this,event,track);
    };

    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
    };

    this.setStyleForElement = function () {
        $(this.item).parent().addClass("outlineRedKeyboardControl");
    };

    this.removeStyleFromElement = function () {
        $(this.item).parent().removeClass("outlineRedKeyboardControl");
    };
}

function ListRowBlockCheckBoxNavElementCustomCheckSearchField(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    this.actionSpace = function (event, track) {
        event.preventDefault();
        $(this.item).parent().prop('checked', true);
        $("#listRowBlock").ajaxStop(function () {

            var curSectionName = mainNavigationTrack.currentPosition.currentSectionName;

            var func = mainNavigationTrack.blockNameToSelectorMapper[curSectionName];
            var reloadedSectionElemnets = func;

            mainNavigationTrack.addSection(curSectionName, reloadedSectionElemnets);
            mainNavigationTrack.getCurrentElement().setFocus();
        });
        this.action.call(this, event, track);

    };

    var _parentAction = this.action;
    this.action = function (event, track) {
        _parentAction.call(this, event, track);
    };

    this.setStyleForElement = function () {
        $(this.item).parent().addClass("outlineRedKeyboardControl");
    };

    this.removeStyleFromElement = function () {
        $(this.item).parent().removeClass("outlineRedKeyboardControl");
    };
}

function CheckBoxNavElementCheckboxDocsSearchField(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    this.actionSpace = function (event,track) {
        event.preventDefault();
        $(this.item).parent().prop('checked', true);
        this.action.call(this,event,track);
    };

    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
    };

    this.setStyleForElement = function () {
        $(this.item).addClass("outlineRedKeyboardControl")
    };

    this.removeStyleFromElement = function () {
        $(this.item).removeClass("outlineRedKeyboardControl")
    };
}

function ReloadedNavElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);
    this.reloadebleSectionSelector=null;

    this.reloadSection = function () {
        $(":animated").promise().done(function () { //wait until all animaitons finished
            var curSectionName = mainNavigationTrack.currentPosition.currentSectionName;
            var currElement = mainNavigationTrack.getCurrentElement();

            var isPopUp = $("div.popUp").length > 0;
            var reloadedSectionElemnets = null;
            if (isPopUp) {
                reloadedSectionElemnets = function () {
                    return componentsCollector("div.popUp");
                };
            } else {
                var func=mainNavigationTrack.blockNameToSelectorMapper[curSectionName];
                reloadedSectionElemnets = func;
            }
            mainNavigationTrack.addSection(curSectionName, reloadedSectionElemnets);

            var newCurEl = mainNavigationTrack.findElementPositionInSection(curSectionName, currElement);
            mainNavigationTrack.currentPosition.currentSectionName = curSectionName;
            mainNavigationTrack.currentPosition.positionInCurrentSection = newCurEl;
        });
    };

    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
        this.reloadSection();

    }
}

function SetParametersLinkNavElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
        $(":animated").promise().done(function () { //wait until all animaitons finished
            var curSectionName = mainNavigationTrack.currentPosition.currentSectionName;
            var reloadedSectionElemnetsFunc = function (){ return getSearchFormElements();};

            mainNavigationTrack.addSection(curSectionName, reloadedSectionElemnetsFunc);
            mainNavigationTrack.getCurrentElement().setFocus();
        });
    }
}

function DatePickerSelectNavElement(/*element*/ item, datePicker, section) {
    AbstractNavElement.apply(this, arguments);
    this.datePicker = datePicker;
    this.section = section;
    var self = this;
    this.select = false;
    this.isSelectDropDown = false;

    this.setIsSelectDropDown = function (value) {
        if ($.browser.msie) {
            this.isSelectDropDown = value;
        } else {
            this.isSelectDropDown = false;
        }
    };

    var _parentSetFocus = this.setFocus;
    this.setFocus = function () {
        this.select = true;
        _parentSetFocus.call(this, arguments);
        $(this.item).focus();
    };

    var _parentLoseFocus = this.loseFocus;
    this.loseFocus = function () {
        this.select = false;
        self.setIsSelectDropDown(false);
        _parentLoseFocus.call(this, arguments);
        $(this.item).blur();
    }

    this.action = function (event,track) {
        event.preventDefault();
        this.loseFocus();
        var prevElement = track.next();
        prevElement.setFocus();
    };
    $(this.item).on("change", function () {
        $(self.section.item).blur();
        self.loseFocus();
        track = datePicker.createNavigationTrack();
        track.getCurrentElement().loseFocus();
        track.sectionFirstElement(section);
        var element = track.next();
        element.setFocus();
    });
    $(this.item).on("blur", function () {
        if (self.select){
            $(self.item).focus();
        }
        self.setIsSelectDropDown(false);
    });

    this.actionSpace=function(event,track){
        self.setIsSelectDropDown(true);
    };

    this.actionDown=function(event,track){
        if (!self.isSelectDropDown) {
            event.preventDefault();
            var length = $(this.item).children("option").length - 1;
            var position = $(this.item).children("option").index($(this.item).children("option:selected")) + 1;
            position = position > length ? 1 : position + 1;
            $(this.item).children("option:selected").attr("selected", false);
            $(this.item).children(":nth-child(" + position + ")").attr("selected", true);
            $(this.item).click();
            var month = $("#ui-datepicker-div .ui-datepicker-month :selected").val();
            var year = $("#ui-datepicker-div .ui-datepicker-year :selected").val();
            $(datePicker.item).datepicker('setDate', new Date(year, month, 1));
            track = datePicker.createNavigationTrack();
            track.getCurrentElement().loseFocus();
            track.sectionFirstElement(section).setFocus();
        }
    };
    this.actionUp=function(event,track){
        if (!self.isSelectDropDown) {
            event.preventDefault();
            var length = $(this.item).children("option").length - 1;
            var position = $(this.item).children("option").index($(this.item).children("option:selected")) - 1;
            position = position < 0 ? length + 1 : position + 1;
            $(this.item).children("option:selected").attr("selected", false);
            $(this.item).children(":nth-child(" + position + ")").attr("selected", true);
            $(this.item).click();
            var month = $("#ui-datepicker-div .ui-datepicker-month :selected").val();
            var year = $("#ui-datepicker-div .ui-datepicker-year :selected").val();
            $(datePicker.item).datepicker('setDate', new Date(year, month, 1));
            track = datePicker.createNavigationTrack();
            track.getCurrentElement().loseFocus();
            track.sectionFirstElement(section).setFocus();
        }
    };
    this.actionLeft = function (event,track) {
        event.preventDefault();
    };
    this.actionRighth = function (event,track) {
        event.preventDefault();
    };
    this.actionEscape = function (event,track) {
        event.preventDefault();
    };
}

function DatePickerDayNavElement(/*element*/ item, datePicker) {
    AbstractNavElement.apply(this, arguments);
    this.datePicker = datePicker;

    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this, event, track);
        $(datePicker.item).attr("disabled", false);
        this.datePicker.focus();
        innerNavigationTrack = clearTrack(innerNavigationTrack);
    }

    this.actionTab = function (event,track){
        event.preventDefault();
        this.loseFocus();
        var nextElement = track.sectionFirstElement(datePicker.parts.month);
        nextElement.setFocus(true);
    };

    this.actionShiftTab = function (event,track){
        event.preventDefault();
        this.loseFocus();
        var prevElement =  track.sectionLastElement(datePicker.parts.year);
        prevElement.setFocus(false);
    };

    this.actionLeft = function (event,track) {
        event.preventDefault();
        this.loseFocus();
        var element = track.getPrevOrNextCurrentSectionElement(-1);
        element.setFocus();
    };

    this.actionRighth = function (event,track) {
        event.preventDefault();
        this.loseFocus();
        var element = track.getPrevOrNextCurrentSectionElement(1);
        element.setFocus();
    };

    this.actionDown=function(event,track){
        event.preventDefault();
        this.loseFocus();
        var element = track.getPrevOrNextCurrentSectionElementVerticalNavigation(7, 1);
        element.setFocus();
    };
    this.actionUp=function(event,track){
        event.preventDefault();
        this.loseFocus();
        var element = track.getPrevOrNextCurrentSectionElementVerticalNavigation(7, -1);
        element.setFocus();
    };
    this.actionSpace=function(event,track){
        event.preventDefault();
    };
}

function DatePikerNavElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);
    var self = this;
    this.parts = {
        month : "monthSection",
        year : "yearSection",
        days : "daysSection"
    };

    this.disableHandlers = function () {
        $(this.item).off('focus', $.datepicker._showDatepicker);
        $(this.item).off('keydown', $.datepicker._doKeyDown);
        $(document).off('mousedown', $.datepicker._checkExternalClick);
    };

    this.enableHandlers = function () {
        $(this.item).on('focus', $.datepicker._showDatepicker);
        $(this.item).on('keydown', $.datepicker._doKeyDown);
        $(document).on('mousedown', $.datepicker._checkExternalClick);
    };

    this.createNavigationTrack = function () {
        var innerItems=$("#ui-datepicker-div").find(".ui-datepicker-month, .ui-datepicker-year, .ui-state-default");
        var arr = innerItems.toArray();
        var year = [],
            month = [],
            daysSection = [];
        arr.forEach(function (item) {
            switch (true){
                case $(item).is(".ui-datepicker-month"):
                    var el = new DatePickerSelectNavElement(item, self, self.parts.month);
                    month.push(el);
                    break;
                case $(item).is(".ui-datepicker-year"):
                    var el = new DatePickerSelectNavElement(item, self, self.parts.year);
                    year.push(el);
                    break;
                case $(item).is("a"):
                    var el = new DatePickerDayNavElement(item, self);
                    daysSection.push(el);
                    break;
                default:
                    return;
            }
        });
        innerNavigationTrack = new FocusesElementsMap();
        innerNavigationTrack.addSection(self.parts.month, function(){return month});
        innerNavigationTrack.addSection(self.parts.year, function(){return year});
        innerNavigationTrack.addSection(self.parts.days, function(){return daysSection});
        innerNavigationTrack.getCurrentElement().setFocus();
        $(this.item).blur();
        return innerNavigationTrack;
    };

    this.focus = function () {
        $(this.item).focus();
    };
    this.setFocus = function () {
        this.setStyleForElement(this.item);
        this.disableHandlers();
        $(this.item).focus();
    };

    this.loseFocus = function () {
        this.removeStyleFromElement(this.item);
        this.disableHandlers();
        this.enableHandlers();
        $(this.item).attr("disabled", false);
        innerNavigationTrack = clearTrack(innerNavigationTrack);
        $(this.item).blur();
    };

    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
        $(this.item).attr("disabled", true);
        $.datepicker._showDatepicker(this.item);
        $(":animated").promise().done(function () {
            self.createNavigationTrack();
        });
    };
}


function TabElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);
}

function BtnElement(/*element*/ item) {
    ReloadedNavElement.apply(this, arguments);

}

function LinkPopUpElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    var _parentSetFocus = this.setFocus;
    this.setFocus = function () {
        _parentSetFocus.call(this);
        if ($.browser.msie) {
            $(this.item).focus();
            window.focus();
        }
    }
}

function RadioButtonElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);
    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
    };

    this.actionSpace = function (event,track) {
        event.preventDefault();
        $(item).trigger('click',[1]);
    };

    this.setStyleForElement = function () {
        $(this.item).addClass("outlineRedKeyboardControl")
    };

    this.removeStyleFromElement = function () {
        $(this.item).removeClass("outlineRedKeyboardControl")
    };

}


function RadioButtonElementPoorVision(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);
    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
    };

    this.actionSpace = function (event,track) {
        event.preventDefault();
        $(item).trigger('click',[1]);
    };

    this.setStyleForElement = function () {
        $(this.item).addClass("outlineRedKeyboardControl")
    };

    this.removeStyleFromElement = function () {
        $(this.item).removeClass("outlineRedKeyboardControl")
    };

}

function PoorVisionPseudoRadioButtonElement(item) {
    AbstractNavElement.apply(this, arguments);
    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
    };

    this.actionSpace = function (event,track) {
        event.preventDefault();
        $(item).siblings("input[type='radio']").trigger('click',[1]);
    };

    this.setStyleForElement = function () {
        $(this.item).addClass("outlineRedKeyboardControlBefore")
    };

    this.removeStyleFromElement = function () {
        $(this.item).removeClass("outlineRedKeyboardControlBefore")
    };
}

function TuningQuickSearchBlockNavElement(item) {
    AbstractNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {
        var tuningQuickSearchBox=$(item).next(".tuningQuickSearchBox");
        var isVisible=$(tuningQuickSearchBox).is(":visible");
        _parentAction.call(this,event,track);
        if (!isVisible){
            var innerItems=$(tuningQuickSearchBox).find(".tuningQuickSearchList li span.tuningQuickSearchTitle");
            var arr = innerItems.toArray();
            var result = [];
            arr.forEach(function (item) {
                var el = new PseudoSelectorNavElement(item);
                result.push(el);
            });
            var func=function(){return result};
            innerNavigationTrack=clearTrack(innerNavigationTrack);
            innerNavigationTrack = new FocusesElementsMap();
            innerNavigationTrack.addSection("singleTrack", func);
            innerNavigationTrack.getCurrentElement().setFocus();
        }
    };

    var _parentLoseFocus = this.loseFocus;
    this.loseFocus = function () {
        _parentLoseFocus.call(this);
        var tuningQuickSearchBox=$(item).next(".tuningQuickSearchBox");
        var isVisible=$(tuningQuickSearchBox).is(":visible");
        if (isVisible){
            $(item).trigger('click',[1]);
        }
        innerNavigationTrack=clearTrack(innerNavigationTrack);
    }
}

function ExtendedTuningQuickSearchBlockNavElement(item) {
    AbstractNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {
        var tuningQuickSearchBox=$(item).next(".tuningQuickSearchBox");
        var isVisible=$(tuningQuickSearchBox).is(":visible");
        _parentAction.call(this,event,track);
        if (!isVisible){
            var innerItems=$(tuningQuickSearchBox).find(".tuningQuickSearchList li").filter(function(i,e){
                return !($(e).css('display') == 'none');
            }).find("span.tuningQuickSearchTitle, span.editingBtn, span.delBtn");
            var arr = innerItems.toArray();
            var result = [];
            arr.forEach(function (item) {
                switch(true){
                    case $(item).is(".editingBtn, .delBtn") && usePoorVisionOption():
                        var el = new PseudoSelectorControlNavElement(item);
                        break;
                    default:
                        var el = new PseudoSelectorNavElement(item);
                        break;
                }
                result.push(el);
            });
            var func=function(){return sortSection(result, "extendedTuningQuickSearchBlock")};
            innerNavigationTrack=clearTrack(innerNavigationTrack);
            innerNavigationTrack = new FocusesElementsMap();
            innerNavigationTrack.addSection("singleTrack", func);
            innerNavigationTrack.getCurrentElement().setFocus();
        }
    };

    var _parentLoseFocus = this.loseFocus;
    this.loseFocus = function () {
        _parentLoseFocus.call(this);
        var tuningQuickSearchBox=$(item).next(".tuningQuickSearchBox");
        var isVisible=$(tuningQuickSearchBox).is(":visible");
        if (isVisible){
            $(item).trigger('click',[1]);
        }
        innerNavigationTrack=clearTrack(innerNavigationTrack);
    }
}

function ChooseTableElement(item){
    AbstractNavElement.apply(this, arguments);

    var _parentSetFocus = this.setFocus;
    this.setFocus = function () {
        _parentSetFocus.call(this);
        var tableRows= $(this.item).find("tr");
        var arr = tableRows.toArray();
        var result = [];
        arr.forEach(function (item) {
            var el = new ChooseTableRowElement(item);
            result.push(el);
        });
        var func=function(){return result};
        innerNavigationTrack = new FocusesElementsMap();
        innerNavigationTrack.addSection("singleTrack", func);
        innerNavigationTrack.getCurrentElement().setFocus();


    };

    var _parentLoseFocus = this.loseFocus;
    this.loseFocus = function () {
        _parentLoseFocus.call(this);
        innerNavigationTrack=clearTrack(innerNavigationTrack);
    };

    this.setStyleForElement = function () {
        //do nothing
    };

    this.removeStyleFromElement = function () {
        //do nothing
    };

};

function PurchaseMethodCheckBoxNavElement(item) {
    CheckBoxNavElement.apply(this, arguments);

    var _parentSetFocus = this.setFocus;
    this.setFocus = function () {
        _parentSetFocus.call(this);
        var self = usePoorVisionOption() ? $(this.item).parent() : this.item;
        scrollToRelativeElement(self, $(".choiceTable").parent("div")[0]);
    }
};

function ChoosePurchaseMethodElement(item){
    AbstractNavElement.apply(this, arguments);

    var _parentSetFocus = this.setFocus;
    this.setFocus = function () {
        _parentSetFocus.call(this);
        var tableRows= $(this.item).find(":checkbox");
        var arr = tableRows.toArray();
        var result = [];
        arr.forEach(function (item) {
            var el = new PurchaseMethodCheckBoxNavElement(item);
            result.push(el);
        });
        var func=function(){return result};
        innerNavigationTrack = new FocusesElementsMap();
        innerNavigationTrack.addSection("singleTrack", func);
        innerNavigationTrack.getCurrentElement().setFocus();


    };

    var _parentLoseFocus = this.loseFocus;
    this.loseFocus = function () {
        _parentLoseFocus.call(this);
        innerNavigationTrack=clearTrack(innerNavigationTrack);
    };

    this.setStyleForElement = function () {
        //do nothing
    };

    this.removeStyleFromElement = function () {
        //do nothing
    };

}

function ChooseTableRowElement(item){
    AbstractNavElement.apply(this, arguments);

    var _parentAction=this.action;
    this.action=function(event,track){
        _parentAction.call(this,event,track);
        var enabledButton=$("div.popUp input:button:enabled.bigOrangeBtn");
        var enabledButtonObject=new BtnElement($(enabledButton));
        mainNavigationTrack.navigationBloksMap.popUpElements.push(enabledButtonObject);
    };

    this.setFocus=function(){
        this.setStyleForElement(this.item);
        var container = $(this.item).closest(".wrapperTableHeight");
        scrollToRelativeElement(this.item,container);
    };

    this.setStyleForElement = function () {
        $(this.item).addClass("choiceTableKeyboardControl");
    };

    this.removeStyleFromElement = function () {
        $(this.item).removeClass("choiceTableKeyboardControl");
    };
}

function ChooseOrganisationPaginator(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {
        event.preventDefault();
        var url = $(item).attr("href");
        if (url != null) {
            window.location.href = url;
        }
        $("div.popUp").ajaxComplete(function() {
            startPopUp(mainNavigationTrack.currentPosition);
        });

    };

    var _parentSetFocus = this.setFocus;
    this.setFocus = function () {
        _parentSetFocus.call(this);
        if ($(".choiceTable").parent("div").length > 0) {
            scrollToRelativeElement(this.item, $(".choiceTable").parent("div")[0]);
        }
    };
};

function ToolTipMenuNavElement(item) {
    AbstractNavElement.apply(this, arguments);
    this.action = function (event,track) {
        $(this.item).addClass("showToolTipKeyboardControl");
        var subElements = $(this.item).find("ul li");
        var arr = subElements.toArray();
        var result = [];
        arr.forEach(function (item) {
            var el = new AbstractNavElement(item);
            result.push(el);
        });
        var func = function (){return result};
        innerNavigationTrack = new FocusesElementsMap();
        innerNavigationTrack.addSection("singleTrack", func);
        innerNavigationTrack.getCurrentElement().setFocus();
    };

    this.setStyleForElement = function () {
        $(this.item).addClass("outlineRedKeyboardControl");
    };

    this.removeStyleFromElement = function () {
        $(this.item).removeClass("outlineRedKeyboardControl").removeClass("showToolTipKeyboardControl");
    };
    var _parentLoseFocus = this.loseFocus;
    this.loseFocus = function () {
        _parentLoseFocus.call(this);
        innerNavigationTrack=clearTrack(innerNavigationTrack);
    };

}

function HorizontalScrollableElement(item) {
    AbstractNavElement.apply(this, arguments);
    this.scrollLeft = 0;
    this.offset = 40;

    this.actionLeft = function (event,track) {
        if (this.scrollLeft - this.offset > 0) {
            this.scrollLeft -= this.offset;
        } else {
            this.scrollLeft = 0;
        }
        $(this.item).parent().scrollLeft(this.scrollLeft);
    };

    this.actionRighth = function (event,track) {
        var hideLength = $(this.item).width() - $(this.item).parent().width();
        if (this.scrollLeft + this.offset < hideLength) {
            this.scrollLeft += this.offset;
        } else {
            this.scrollLeft = hideLength;
        }
        $(this.item).parent().scrollLeft(this.scrollLeft);
    };

    this.setStyleForElement = function () {
        $(this.item).closest(".noticeTabBoxWrapper").addClass("outlineRedKeyboardControl");
    };

    this.removeStyleFromElement = function () {
        $(this.item).closest(".noticeTabBoxWrapper").removeClass("outlineRedKeyboardControl");
    };
}

function CabinetElement(item) {
    AbstractNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
        if (($(this.item).children().length > 0)) {
            if ($.cookie('usePoorVisionOption') != 'true') {
                var subElements = $(this.item).children().children(".cabinetItem");
                var arr = subElements.toArray();
                var result = [];
                arr.forEach(function (item) {
                    var el = new LinkLKElement(item);
                    result.push(el);
                });
                var func=function(){return result};
                innerNavigationTrack = new FocusesElementsMap();
                innerNavigationTrack.addSection("singleTrack", func);
                innerNavigationTrack.getCurrentElement().setFocus();
            } else {
                var subElements = $(this.item).children().find(".cabinetItem");
                var arr = subElements.toArray();
                var result = [];
                arr.forEach(function (item) {
                    var el = new LinkLKElement(item);
                    result.push(el);
                });
                var func=function(){return result};
                innerNavigationTrack = new FocusesElementsMap();
                innerNavigationTrack.addSection("singleTrack", func);
                innerNavigationTrack.getCurrentElement().setFocus();
            }

        }
    };

    var _parentLoseFocus = this.loseFocus;
    this.loseFocus = function () {
        _parentLoseFocus.call(this);
        $("div.personalCabinetPopUp").fadeOut(300);
        if (innerNavigationTrack != null && "singleTrack" in innerNavigationTrack.navigationBloksMap) {
            var curInnerElement = innerNavigationTrack.getCurrentElement();
            curInnerElement.loseFocus();
            innerNavigationTrack = null;
        }

    }

}

function ShowCountElement(item) {
    AbstractNavElement.apply(this, arguments);
    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
        var subElements = $(this.item).parent().children("ul").children("li");
        var arr = subElements.toArray();
        var result = [];
        arr.forEach(function (item) {
            var el = new ChooseShowCountElement(item);
            result.push(el);
        });
        var func=function(){return result};
        innerNavigationTrack = new FocusesElementsMap();
        innerNavigationTrack.addSection("singleTrack", func);
        innerNavigationTrack.getCurrentElement().setFocus();

    };

    var _parentLoseFocus = this.loseFocus;
    this.loseFocus = function () {
        _parentLoseFocus.call(this);
        //$("div.personalCabinetPopUp").fadeOut(300);
        if (innerNavigationTrack != null && "singleTrack" in innerNavigationTrack.navigationBloksMap) {
            var curInnerElement = innerNavigationTrack.getCurrentElement();
            curInnerElement.loseFocus();
            innerNavigationTrack = null;
        }

    };

    this.setStyleForElement = function () {
        $(this.item).children().first().addClass("outlineRedKeyboardControl")
    };

    this.removeStyleFromElement = function () {
        $(this.item).children().first().removeClass("outlineRedKeyboardControl")
    };
}


function ExpandCollapseElement(/*element*/ item) {
    ReloadedNavElement.apply(this, arguments);


}

function PoorVisionCheckbox(/*element*/ item) {
    CheckBoxNavElement.apply(this, arguments);

    this.setStyleForElement = function () {
        $(this.item).next("label").addClass("borderColorRedKeyboardControl");
    };

    this.removeStyleFromElement = function () {
        $(this.item).next("label").removeClass("borderColorRedKeyboardControl");
    };
}

function FooterExpandCollapseElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
        var func=function(){return getFooterElementsNew()};
        $(":animated").promise().done(function () {
            if ($(this.item).is("expandBox")) {
                mainNavigationTrack.addSection("footer",func );
            } else {
                delete mainNavigationTrack["footer"];
                mainNavigationTrack.addSection("footer", func);
            }
        });
    }

}

function ExcludeSearchTextFieldElement(/*element*/ item) {
    TextNavElement.apply(this, arguments);

    var _parentSetFocus=this.setFocus;
    this.setFocus=function(upOrDown){

        if (!($(this.item).is(":disabled"))){
            _parentSetFocus.call(this);
        } else {
            if (upOrDown!=undefined){
                if(upOrDown){
                    mainNavigationTrack.next();
                } else {
                    mainNavigationTrack.prev();
                }
                mainNavigationTrack.getCurrentElement().setFocus();
            }
        }
    };

}

function PoorVisionControlNavElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    var _parentSetFocus = this.setFocus;
    this.setFocus = function () {
        _parentSetFocus.call(this);
        if ($.browser.msie) {
            window.focus();
            $(this.item).focus();
        }
    }
}

function ManySelectNavElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);
    var currentObj=this;
    var _parentAction=this.action;
    this.action = function (event,track) {
        var isOpen=$(this.item).next(".selectChose").is(":visible");
        _parentAction.call(this,event,track);
        $(":animated").promise().done(function () { //wait until all animaitons finished

            var curSectionName = mainNavigationTrack.currentPosition.currentSectionName;
            var currElement = mainNavigationTrack.getCurrentElement();

            var isPopUp = $("div.popUp").length > 0;
            var reloadedSectionElemnets = null;
            if (isPopUp) {
                reloadedSectionElemnets = function () {
                    return componentsCollector("div.popUp");
                };
            } else {
                var func=mainNavigationTrack.blockNameToSelectorMapper[curSectionName];
                reloadedSectionElemnets = func;
            }
            mainNavigationTrack.addSection(curSectionName, reloadedSectionElemnets);

            var newCurEl = mainNavigationTrack.findElementPositionInSection(curSectionName, currElement);
            mainNavigationTrack.currentPosition.currentSectionName = curSectionName;
            mainNavigationTrack.currentPosition.positionInCurrentSection = newCurEl;

            var subMenu = $(currentObj.item).next().find("li:visible");


            if (isOpen){
                return;
            }

            var arr = subMenu.toArray();
            var result = [];
            arr.forEach(function (item) {
                var el = new ManySelectorCheckBoxNavElement($(item).find(":checkbox"));
                result.push(el);
            });
            var func=function(){return result};
            innerNavigationTrack = new FocusesElementsMap();
            innerNavigationTrack.addSection("singleTrack", func);
            innerNavigationTrack.getCurrentElement().setFocus();
        });
    };
    var _parentLoseFocus = this.loseFocus;
    this.loseFocus = function () {
        _parentLoseFocus.call(this);
        if (innerNavigationTrack!=null) {
            innerNavigationTrack.getCurrentElement().loseFocus();
            innerNavigationTrack = null;
        }
    }

}
function TuningIcoElement(item) {
    AbstractNavElement.apply(this, arguments);
    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
        $(this.item).find("ul").show();
        var result = [];
        var subPopupElements = $(this.item).find("li").find("a.linkPopUp");
        var arr = subPopupElements.toArray();
        arr.forEach(function (item) {
            var el = new LinkPopUpElement(item);
            result.push(el);
        });
        var subElements = $(this.item).find("li").find("a.handlerClickForClearCookie");
        var arr = subElements.toArray();
        arr.forEach(function (item) {
            var el = new LinkElement(item);
            result.push(el);
        });
        var func=function(){return result};
        innerNavigationTrack = new FocusesElementsMap();
        innerNavigationTrack.addSection("singleTrack", func);
        innerNavigationTrack.getCurrentElement().setFocus();

    };

    var _parentLoseFocus = this.loseFocus;
    this.loseFocus = function () {
        _parentLoseFocus.call(this);
        $(this.item).find("ul").hide();
        if (innerNavigationTrack != null && "singleTrack" in innerNavigationTrack.navigationBloksMap) {
            var curInnerElement = innerNavigationTrack.getCurrentElement();
            curInnerElement.loseFocus();
            innerNavigationTrack = null;
        }
    }
}

function DownloadedDocumentIconElement(item) {
    LinkElement.apply(this, arguments);

    this.setStyleForElement = function () {
        $(this.item).find("img").addClass("outlineRedKeyboardControl");
    };

    this.removeStyleFromElement = function () {
        $(this.item).find("img").removeClass("outlineRedKeyboardControl");
    };
}

function CollapceBoxElement(item) {
    ReloadedNavElement.apply(this, arguments);
}


function InputRadioMainPage(/*element*/ item) {
    ReloadedNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {
        event.preventDefault();
        _parentAction.call(this,event,track);
    };

    this.setStyleForElement = function () {
        $(this.item).parent().addClass("outlineRedKeyboardControl");
    };

    this.removeStyleFromElement = function () {
        $(this.item).parent().removeClass("outlineRedKeyboardControl");
    };
}

function InnerBannerElement(/*element*/ item) {
    BannerElement.apply(this, arguments);


    this._getCurrentBannerSection=function(){
        return $(this.item).closest(".items-overflow").siblings(".backgroundColorEFF0F1.marginTop13px.paddingTop12px.textAlignCenter").find("li.active").index();
    };

    this._getVisibleBannerSection=function(){
        var index=this._getCurrentBannerSection();
        var visibleSubSecton=$(this.item).closest(".items-lists").find(".item").get(index);
        return visibleSubSecton;
    };
}

function TopRightArrowMainPage(/*element*/ item) {
    RightArrowMainPage.apply(this, arguments);
    this._getArrowsNumber=function(){
        var curData=$('.handlerInnerHtmlWidget').find(".items-overflow.first-level").attr('data-cur');
        var selector=".pageCarouselHandler.items-parent .items-lists .item.item-page-"+curData;
        var number=$(selector).find("div.items-controls:first").children("span").length;
        return number;
    };
}

function TopLeftArrowMainPage(/*element*/ item) {
    LeftArrowMainPage.apply(this, arguments);
    this._getArrowsNumber=function(){
        var curData=$('.handlerInnerHtmlWidget').find(".items-overflow.first-level").attr('data-cur');
        var selector=".pageCarouselHandler.items-parent .items-lists .item.item-page-"+curData;
        var number=$(selector).find("div.items-controls:first").children("span").length;
        return number;
    };
}

function RightArrowMainPage(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);
    var _parentAction = this.action;
    var _currentElement=this;
    this.action = function (event,track) {
        var arrowNumberBefore=this._getArrowsNumber();
        _parentAction.call(this,event,track);
        $(":animated").promise().done(function () { //wait until all animaitons finished
            var curSectionName = mainNavigationTrack.currentPosition.currentSectionName;
            var currPos = mainNavigationTrack.currentPosition.positionInCurrentSection;
            var func=mainNavigationTrack.blockNameToSelectorMapper[curSectionName];
            var reloadedSectionElemnets = func;
            mainNavigationTrack.addSection(curSectionName, reloadedSectionElemnets);
            var arrowNumberAfter=_currentElement._getArrowsNumber();
            if ( arrowNumberBefore > arrowNumberAfter ){
                _currentElement.loseFocus();
                mainNavigationTrack.prev();
                mainNavigationTrack.getCurrentElement().setFocus();
            } else if (arrowNumberBefore < arrowNumberAfter){
                _currentElement.loseFocus();
                mainNavigationTrack.next();
                mainNavigationTrack.getCurrentElement().setFocus();
            } else {
                _currentElement.loseFocus();
                mainNavigationTrack.getCurrentElement().setFocus();
            }

        });

    };
    this._getArrowsNumber=function(){
        var curData=$('.handlerInnerHtmlWidget').find(".items-overflow.first-level").attr('data-cur');
        var selector=".pageCarouselHandler.items-parent .items-lists .item.item-page-"+curData;
        var number=$(selector).find("div.graph:not(.display-none) div.subCarousel:first").children("div.slick-arrow:not(.slick-disabled)").length;
        return number;
    };
    var _parentLoseFocus=this.loseFocus;
    this.loseFocus = function(){
        _parentLoseFocus.call(this);
        $(document).off("widget.load");
        $(document).off("subwidget.load");


    };
    var _parentSetFocus=this.setFocus;
    this.setFocus = function(){
        _parentSetFocus.call(this);
        $(document).on("widget.load",reloadWidgetBlock);
        $(document).on("subwidget.load",reloadWidgetBlock);
    }
}

function LeftArrowMainPage(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);
    var _parentAction = this.action;
    var _currentElement=this;
    this.action = function (event,track) {
        //var _len1=mainNavigationTrack.getCurrentSectionLength();
        var arrowNumberBefore=this._getArrowsNumber();
        _parentAction.call(this,event,track);
        $(":animated").promise().done(function () { //wait until all animaitons finished
            var curSectionName = mainNavigationTrack.currentPosition.currentSectionName;
            var currPos = mainNavigationTrack.currentPosition.positionInCurrentSection;
            var func=mainNavigationTrack.blockNameToSelectorMapper[curSectionName];
            var reloadedSectionElemnets = func;
            mainNavigationTrack.addSection(curSectionName, reloadedSectionElemnets);
            var arrowNumberAfter=_currentElement._getArrowsNumber();
            if ( arrowNumberBefore > arrowNumberAfter ){
                _currentElement.loseFocus();
                mainNavigationTrack.getCurrentElement().setFocus();
            } else if (arrowNumberBefore < arrowNumberAfter){
                _currentElement.loseFocus();
                mainNavigationTrack.getCurrentElement().setFocus();
            } else {
                _currentElement.loseFocus();
                mainNavigationTrack.getCurrentElement().setFocus();
            }
        });

    };
    this._getArrowsNumber=function(){
        var curData=$('.handlerInnerHtmlWidget').find(".items-overflow.first-level").attr('data-cur');
        var selector=".pageCarouselHandler.items-parent .items-lists .item.item-page-"+curData;
        var number=$(selector).find("div.graph:not(.display-none) div.subCarousel:first").children("div.slick-arrow:not(.slick-disabled)").length;
        return number;
    };
    var _parentLoseFocus=this.loseFocus;
    this.loseFocus = function(){
        _parentLoseFocus.call(this);
        $(document).off("widget.load");
        $(document).off("subwidget.load");
    };
    var _parentSetFocus=this.setFocus;
    this.setFocus = function(){
        _parentSetFocus.call(this);
        $(document).on("widget.load",reloadWidgetBlock);
        $(document).on("subwidget.load",reloadWidgetBlock);
    }
}

function BannerElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {
        event.preventDefault();
        _parentAction.call(this,event,track);
    };

    this._getCurrentBannerSection=function(){
        return $(this.item).closest(".carousel").siblings(".pagingList").find("li.active").index();
    };

    this._getVisibleBannerSection=function(){
        var index=this._getCurrentBannerSection();
        var visibleSubSection=$(this.item).closest(".carousel").find("li").get(index);
        return visibleSubSection;
    };
    var _parentSetFocus=this.setFocus;
    this.setFocus=function(upOrDown){
        _parentSetFocus.call(this);
        var vSection=this._getVisibleBannerSection();
        var cont=$(this.item).is($(vSection)) || $.contains(vSection,this.item);
        if (cont){
            _parentSetFocus.call(this);
        } else {
            if (upOrDown!=undefined){
                if(upOrDown){
                    mainNavigationTrack.getCurrentElement().loseFocus();
                    mainNavigationTrack.next();
                    mainNavigationTrack.getCurrentElement().setFocus(true);
                } else {
                    mainNavigationTrack.getCurrentElement().loseFocus();
                    mainNavigationTrack.prev();
                    mainNavigationTrack.getCurrentElement().setFocus(false);
                }
            }
        }
    };

}

function BannerPagingListElement(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {
        event.preventDefault();
        _parentAction.call(this,event,track);
    }
}

function Select2Element(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);
    var current=this;
    this.action = function (event,track) {
        event.preventDefault();
        var realSelect=$(this.item).select2();
        realSelect.select2("open");

    };

    var _parentLoseFocus=this.loseFocus;
    this.loseFocus = function(){
        console.log("loseFocus");
        $(document).off("select2:open");
        $(document).off("select2:close");
        $(document).off("subwidget.load");
        _parentLoseFocus.call(this);

    };

    var _parentSetFocus=this.setFocus;
    this.setFocus = function(){
        $(document).off("select2:open");
        $(document).off("select2:close");
        $(document).off("subwidget.load");
        $(document).on("select2:open", function (e) {
            var test = $(current.item).next(".select2")[0];
            $(test).find("span.select2-selection.select2-selection--single").focus();
        });


        $(document).on("select2:close", function (e) {
            var innerSelect2 = $(current.item).next(".select2")[0];
            $(innerSelect2).find("span.select2-selection.select2-selection--single").blur();
        });
        $(document).on("subwidget.load",reloadWidgetBlock);

        _parentSetFocus.call(this);
    };

    this.setStyleForElement = function () {
        $(this.item).next(".select2").addClass("outlineRedKeyboardControl");
    };

    this.removeStyleFromElement = function () {
        $(this.item).next(".select2").removeClass("outlineRedKeyboardControl");
    };
}



function TopTabsMainPage(/*element*/ item) {
    AbstractNavElement.apply(this, arguments);

    var _parentAction = this.action;
    this.action = function (event,track) {
        _parentAction.call(this,event,track);
    };
    var _parentLoseFocus=this.loseFocus;
    this.loseFocus = function(){
        _parentLoseFocus.call(this);
        $(document).off("widget.load");
        $(document).off("subwidget.load");


    };
    var _parentSetFocus=this.setFocus;
    this.setFocus = function(){
        _parentSetFocus.call(this);
        $(document).on("widget.load",reloadWidgetBlock);
        $(document).on("subwidget.load",reloadWidgetBlock);
    }

}

function reloadWidgetBlock(){
    var nextSectionName="widgetBlock";
    var nextSectionElements=mainNavigationTrack.blockNameToSelectorMapper[nextSectionName];
    mainNavigationTrack.addSection(nextSectionName, nextSectionElements);
}



function scrollToElelment(element,container) {
    if (isScrolledIntoView(element)) {
        return;
    }

    var el = $(element);
    var elOffset = el.offset().top;
    var elHeight = el.height();
    var containerHeight=(container == undefined) ? window : container;
    var containerAnimate=(container== undefined) ? 'html,body' : container;
    var windowHeight = $(containerHeight).height();
    var offset;

    if (elHeight < windowHeight) {
        offset = elOffset - ((windowHeight / 2) - (elHeight / 2));
    }
    else {
        offset = elOffset;
    }

    $(containerAnimate).scroll();
    $(containerAnimate).animate({
        scrollTop: offset
    }, 300);

    return false;
}

function scrollToRelativeElement(element, container) {
    if (isScrolledIntoContainer(element,container)) {
        return;
    }

    var el = $(element);

    var offsetTop = el.position().top - el.closest(container).position().top;
    var realOffsetTop = $(container).scrollTop() + offsetTop;


    $(container).scroll();
    // $(container).animate({
    //     scrollTop: realOffsetTop
    // }, 300);
    $(container).scrollTop(realOffsetTop);

    return false;
}


var menuIsCollapsed = true;

function isScrolledIntoView(elem) {
    var docViewTop = $(window).scrollTop();
    var docViewBottom = docViewTop + $(window).height();

    var elemTop = $(elem).offset().top;
    var elemBottom = elemTop + $(elem).height();

    return ((elemBottom <= docViewBottom) && (elemTop >= docViewTop));
}

function isScrolledIntoContainer(elem,container) {
    var offsetTop = $(elem).position().top - $(elem).closest(container).position().top;
    var realOffsetTop = $(container).scrollTop() + offsetTop;


    var docViewTop = $(container).scrollTop();
    var docViewBottom = docViewTop + $(container).height();

    var elemTop=realOffsetTop;
    var elemBottom = elemTop + $(elem).height();

    return ((elemBottom <= docViewBottom) && (elemTop >= docViewTop));
}


function getHeaderElements() {
    var headerSelector = "div.header";
    var resultArray = new Array();
    $(headerSelector).find("*:visible:not(:animated)").each(function () {
        switch (true) {
            case $(this).is(".headerLinkCaption, a.linkPopUp:has('span')"):
                return;
            case $(this).is(".logo"):
                var element = new LinkElement(this);
                break;
            case $(this).is("a.poorVisionLink, .cardLogo"):
                var element = new AbstractNavElement(this);
                break;
            case $(this).is("a.linkPopUp span"):
                var element = new LinkPopUpElement(this);
                break;
            case $(this).is("a.clientIco"):
                var element = new LinkElementWithDefaultStyle(this);
                break;
            case $(this).is("a:not(#setParametersLink)"):
                var element = new LinkElement(this);
                break;
            case $(this).is("dd.cabinet"):
                var element = new CabinetElement(this);
                break;
            default:
                return;
        }
        resultArray.push(element);

    });

    return resultArray;
}
function getHeaderElementsNew() {
    var headerSelector = "header";
    var resultArray = new Array();
    $(headerSelector).find("*:visible:not(:animated)").each(function () {
        switch (true) {
            case $(this).is(".logo header-offset text-base-micro, a.linkPopUp:has('span')"):
                return;
            case $(this).is(".header-logo__img"):
                var element = new LinkElement(this);
                break;
            case $(this).is(".header-offset"):
                var element = new LinkElement(this);
                break;
            case $(this).is(".phone a"):
                var element = new LinkElement(this);
                break;
            case $(this).is(".wrapper-link a"):
                var element = new LinkElement(this);
                break;
            case $(this).is(".main-item a"):
                var element = new LinkElement(this);
                break;
            case $(this).is(".region"):
                var element = new LinkElement(this);
                break;
            case $(this).is(".glass-icon"):
                var element = new LinkElement(this);
                break;
            case $(this).is(".account"):
                var element = new LinkElement(this);
                break;
            default:
                return;
        }
        resultArray.push(element);

    });

    return resultArray;
}

function getBtnUpElement() {
    var btnUpSelector = $("div.scrollUp:visible");
    var resultArray = [];
    if (btnUpSelector.length > 0) {
        var element = new BtnUpElement($(btnUpSelector));
        resultArray.push(element);
    }
    return resultArray;
}

function getSortByElements() {
    var sortBySelector = "div.sortBy";
    var resultArray = [];
    $(sortBySelector).find("*:visible:not(:animated)").each(function () {
        switch (true) {
            case $(this).is("a.loadSettings"):
                return;
            case $(this).is(".pageSelect span"):
                var element = new ShowCountElement(this);
                break;
            case $(this).is(".sortItem:has(span:not(:empty))"):
                var element = new SortItemElement(this);
                break;
            case $(this).is("a"):
                var element = new AbstractNavElement(this);
                break;
            default:
                return;
        }
        resultArray.push(element);

    });
    return resultArray;
}

function getSearchResultElements() {
        var allLinks = $("div.registerBox:visible");

        var tempLinkArray = allLinks.toArray();
        var resultArray = [];

        tempLinkArray.forEach(function (item) {
            var elem = sortSection(componentsCollector(item), "resultSearch");
            resultArray.push.apply(resultArray, elem);
        });

        return resultArray;
}

function getPaginatorElements(selector) {
    return componentsCollector(selector);
}

function getListRowBlockElements(selector) {
    return componentsCollector(selector);
}

function getFooterElements() {
    var footerSelector = "div.footer";
    var resultArray = new Array();

    var footerExpand = $("#expandFooterBlock");
    var footerExpandElem = new FooterExpandCollapseElement(footerExpand[0]);
    resultArray.push(footerExpandElem);

    $(footerSelector).find("*:visible:not(:animated)").each(function () {
        switch (true) {
            case $(this).is("a.linkPopUp"):
                var element = new LinkPopUpElement(this);
                break;
            case $(this).is("a:not(#setParametersLink)"):
                var element = new LinkElement(this);
                break;


            default:
                return;
        }
        resultArray.push(element);

    });

    return resultArray;
}
function getFooterElementsNew() {
    var footerSelector = "footer";
    var resultArray = new Array();

    var footerExpand = $("#expandFooterBlock");
    var footerExpandElem = new FooterExpandCollapseElement(footerExpand[0]);
    resultArray.push(footerExpandElem);

    $(footerSelector).find("*:visible:not(:animated)").each(function () {
        switch (true) {
            case $(this).is(".poll-button"):
                var element = new LinkElement(this);
                break;
            case $(this).is(".partners-slider__btn_prev"):
                var element = new LinkElement(this);
                break;
            case $(this).is(".footer-nav__item"):
                var element = new LinkElement(this);
                break;
            case $(this).is(".partners-slider__btn_next"):
                var element = new LinkElement(this);
                break;
            case $(this).is(".list-item__link"):
                var element = new LinkElement(this);
                break;
            case $(this).is(".btn-on-top"):
                var element = new LinkElement(this);
                break;
            default:
                return;
        }
        resultArray.push(element);

    });

    return resultArray;
}

function getSearchFormElements() {
    var searchBlockArray=componentsCollector("div.content-main");
    var contentBlock = componentsCollector("section.content-main");
    //var searchBlockArray=componentsCollector("div.searchBlockAll");
    var filterParametrsArray=componentsCollector("div.filterParametrs");
    var filterBlockArray=componentsCollector("div.filterBlock");
    var searchFormArray=searchBlockArray.concat(contentBlock, filterParametrsArray,filterBlockArray);
    return searchFormArray;
}


function getAllLinks(item) {
    var allLinks = $(item).find("a:visible").filter(function () {
        return $(this).parent().is(":not(#setParametersLink)");
    });

    var tempLinkArray = allLinks.toArray();
    var resultArray = [];

    tempLinkArray.forEach(function (item) {
        var elem;
        elem = new LinkElement(item);
        resultArray.push(elem);
    });

    var lotsInfo = $(item).find("td.lotsInfo p");
    var tempLotsInfoArray = lotsInfo.toArray();
    tempLotsInfoArray.forEach(function (item) {
        var elem;
        elem = new ShowListInfoElement(item);
        resultArray.push(elem);
    });

    return resultArray;
}


function componentsCollector(section) {
    var resultArray = [];
    $(section).find("*:visible").each(function () {

        switch (true) {
            case $(this).is("input.search__input"):
                var element = new LinkElement(this);
                break;
            case $(this).is("span.dropdown__text_selected"):
                var element = new LinkElement(this);
                break;
            case $(this).is(".justify-content-end .search-group"):
                var element = new LinkElement(this);
                break;
            case $(this).is("#go-to-all-param"):
                var element = new LinkElement(this);
                break;
            case $(this).is("div .btn-icon"):
                var element = new LinkElement(this);
                break;
            case $(this).is(".figure"):
                var element = new LinkElement(this);
                break;
            case $(this).is(".news-content .news__text"):
                var element = new LinkElement(this);
                break;
            case $(this).is(".pseudoSelect"):
                var element = new TuningQuickSearchBlockNavElement(this);
                break;
            case $(this).is(".pseudoInput"):
                var element = new ExtendedTuningQuickSearchBlockNavElement(this);
                break;
            case $(this).is(".lotsInfo"):
                var element = new LinkElement(this);
                break;
            case $(this).is(".paginator-button"):
                var element = new LinkElement(this);
                break;
            case $(this).is("a .rss-icon"):
                var element = new LinkElement(this);
                break;
            case $(this).is("div.selectBtnStat"):
                var element = new LinkElement(this);
                break;
            case $(this).is("span.params-years__item"):
                var element = new LinkElement(this);
                break;
            case $(this).is("a.link"):
                var element = new LinkElement(this);
                break;
            case $(this).is("#searchButtonsBlock a"):
                var element = new LinkElement(this);
                break;
            case $(this).is("span.resultItem"):
                var element = new LinkElement(this);
                break;
            case $(this).is("#setParametersLink"):
                var element = new LinkElement(this);
                break;
            case $(this).is("span.collapce"):
                var element = new LinkElement(this);
                break;
            case $(this).is("span.expand"):
                var element = new LinkElement(this);
                break;
            case $(this).is("dd.statusWarn"):
                var element = new LinkElement(this);
                break;
            case $(this).is("dt a"):
                var element = new LinkElement(this);
                break;
            case $(this).is("strong.fontWeightNormal"):
                var element = new LinkElement(this);
                break;
            case $(this).is(".boxIcons a"):
                var element = new LinkElement(this);
                break;
            case $(this).is("li a"):
                var element = new LinkElement(this);
                break;
            default:
                return;
        }
        resultArray.push(element);

    });

    return resultArray;
}
function componentsCollectorOld(section) {
    var resultArray = [];
    $(section).find("*:visible").each(function () {

        switch (true) {
            case $(this).is("a.loadSettings, .headerLinkCaption, .manySelect :checkbox,"+
                " .manySelect .customCheckbox, :disabled:not(#exclText), #choosePurchaseMethodData :checkbox, "+
                " #choosePurchaseMethodData .customCheckbox,  div.items-controls:hidden ~ * input[type='radio'], "+
                " div.items-controls:hidden ~ .graph div.subCarousel .slick-arrow"):   //exclude from result set
                return;
            case $(this).is("#choosePurchaseMethodData.choiceTable.displaytagTable:has(tbody)"):
                var element = new ChoosePurchaseMethodElement(this);
                break;
            case $(this).is(".choiceTable.displaytagTable:has(tbody)"):
                var element = new ChooseTableElement(this);
                break;
            case $(this).is(".hasDatepicker"):
                var element = new DatePikerNavElement(this);
                break;
            case $(this).is(".pseudoSelect"):
                var element = new TuningQuickSearchBlockNavElement(this);
                break;
            case $(this).is(".pseudoInput"):
                var element = new ExtendedTuningQuickSearchBlockNavElement(this);
                break;
            case $(this).is(".manySelect .collapsed"):
                var element = new ManySelectNavElement(this);
                break;
            case $(this).is("span.resultItem:has(span)"):
                var element = new DeletableNavElement($(this).children("span")[0]);
                break;
            case $(this).is(".purchasePlansTypesLi :checkbox"):
                var element = new PurchasePlansTypesLiCheckBoxNavElement(this);
                break;
            case $(this).is("#expandLotsInfo, #expandAll"):
                var element = new ExpandInfoElement(this);
                break;
            case $(this).is("#showRules"):
                var element = new ShowRulesElement(this);
                break;
            case $(this).is(".searchBox .customCheckbox"):
                var element = new CheckBoxNavElementCustomCheckboxSearchField(this);
                break;
            case $(this).is(".searchBox .customCheckbox input"):
                return;
            case $(this).is("#listRowBlock .customCheck input"):
                var element = new ListRowBlockCheckBoxNavElementCustomCheckSearchField(this);
                break;
            case $(this).is(".searchBox .customCheck input, span.customCheck input"):
                var element = new CheckBoxNavElementCustomCheckSearchField(this);
                break;
            case $(this).is("div.toolTipMenu"):
                var element = new ToolTipMenuNavElement(this);
                break;
            case $(this).is(".searchBox .accounting input"):
                var element = new CheckBoxNavElementCheckboxDocsSearchField(this);
                break;
            case $(this).is("li#setParametersLink a"):
                var element = new SetParametersLinkNavElement(this);
                break;
            case $(this).is("a") && $(this).children(".orangeBtn, .greyBtn").length > 0:
                return;
            case $(this).is("a.linkPopUp, a.newExternalPopUpLink"):
                var element = new LinkPopUpElement(this);
                break;
            case $(this).is("#saveParameters, .orangeTabs td, .childBanner, #clearAll, .clearAll," +
                " span.btn input:button, span.resetBtn input:reset, .poorVisionLink, .zoom, a.searchButton,"+
                " div.searchField+input:button.searchButton, div.searchField input:button,"+
                " div.saveExtendedSearch span.editIcon, span.showChangeLink, .closePopUp, div.multiIkz, span.expandTr,"+
                " span.contractCommonPrintForm, a.auth44std, .btnBrd, a.showDetailDocumentInfo"):
                var element = new AbstractNavElement(this);
                break;
            case $(this).is("select") && ($(this).next().is("span.select2")):
                var element = new Select2Element(this);
                break;
            case $(this).is("div.displayTableRow .displayTableCell.cursorPointer"):
                var element = new TopTabsMainPage(this);
                break;
            case $(this).is("div.jcarousel div.pagingList li, div.jqcarousel div.pagingList li, "+
                ".handlerPageInnerHtml .items-parent .items-controls-dots li"):
                var element = new BannerPagingListElement(this);
                break;
            case $(this).is("div.jcarousel ul.carousel li a, div.jqcarousel ul.carousel li a"):
                var element = new BannerElement(this);
                break;
            case $(this).is("div.items-controls:visible ~ .graph div.subCarousel .iconLeftArrow.slick-arrow"):
                var element = new LeftArrowMainPage(this);
                break;
            case $(this).is("div.items-controls:visible ~ .graph div.subCarousel .iconRightArrow.slick-arrow"):
                var element = new RightArrowMainPage(this);
                break;
            case $(this).is("div.items-controls span.iconPageLeftArrow"):
                var element = new TopLeftArrowMainPage(this);
                break;
            case $(this).is("div.items-controls span.iconPageRightArrow"):
                var element = new TopRightArrowMainPage(this);
                break;
            case $(this).is("div.handlerPageInnerHtml .items-parent .items-overflow .items-lists a"):
                var element = new InnerBannerElement(this);
                break;
            case $(this).is("#docViewToggle"):
                var element = new ViewInactiveDocElement(this);
                break;
            case $(this).is(".tabsTblParam td[id]:visible, div.cardWrapper .stageBox .arrow, div.arrowButton, h2.toggleTitle"):
                var element = new ReloadedNavElement(this);
                break;
            case $(this).is(".contentTabsWrapper td[tab] span"):
                var element = new TabElement(this);
                break;
            case $(this).is(".noticeBoxExpand h3, .doubleArrowDown, .doubleArrowUp"):
                var element = new ExpandCollapseElement(this);
                break;
            case $(this).is(".manySelect .btnBtn"):
                var element = new ManySelectBtnElement(this);
                break;
            case $(this).is(".bigOrangeBtn, .blueBtn, .bigGreyBtn, .greyBtn, .orangeBtn, .msgButton"):
                var element = new BtnElement(this);
                break;
            case $(this).is("#chooseOrganizationTablePaginator li a, #chooseExtendedAttributeTablePaginatorContainer li a"):
                var element = new ChooseOrganisationPaginator(this);
                break;
            case $(this).is(".contentTabBoxBlock .innerHtml .pagePositionSelect span, .contentTabBoxBlock .innerHtml .pageSelect span"):
                var element = new ShowCountElement(this);
                break;
            case $(this).is("td.lotsInfo a:has(img)"):
                var element = new DownloadedDocumentIconElement(this);
                break;
            case $(this).is("a.rssBox"):
                var element = new LinkElementWithDefaultStyle(this);
                break;
            case $(this).is("a.text-click"):
                var element = new LinkElement(this);
                break;
            case $(this).is("a p"):
                var element = new LinkParentElement(this);
                break;
            case $(this).is("a"):
                if ($(this).attr('href') === undefined && $(this).attr('onclick') === undefined) {
                    return;
                } else {
                    var element = new LinkElement(this);
                }
                break;
            case $(this).is("div.items-controls:visible ~ * input[type='radio']"):
                var element = new InputRadioMainPage(this);
                break;
            case $(this).is(".radio-head:not(checked) + label"):
                var element = new RadioButtonElementPoorVision(this);
                break;
            case usePoorVisionOption() && ($(this).siblings("input[type='radio']").filter(isDisplayNone).length == 1):
                var element = new PoorVisionPseudoRadioButtonElement(this);
                break;
            case $(this).is("input[type='radio']"):
                var element = new RadioButtonElement(this);
                break;
            case $(this).is("#searchAttachedFileTag .customCheckbox:has(:checkbox)") && usePoorVisionOption():
                var element = new SearchAttachedFileTagCheckBoxNavElement($(this).children(":checkbox")[0]);
                break;
            case $(this).is(".customCheckbox:has(:checkbox)") && usePoorVisionOption():
                var element = new PoorVisionCheckbox($(this).children(":checkbox")[0]);
                break;
            case $(this).is("#selectAllParameters:checkbox, #searchParamChooseTable :checkbox"):
                var element = new PopUpCheckBoxNavElement(this);
                break;
            case $(this).is("input#exclText:text"):
                var element = new ExcludeSearchTextFieldElement(this);
                break;
            case $(this).is("#regUserDiv input:text, #regUserDiv input:password"):
                var element = new PopUpTextInputElement(this);
                break;
            case $(this).is("input:text[maxlength]:not([maxlength=''])"):
                var element = new TextInputWithValidationElement(this);
                break;
            case $(this).is("input:text, input:password"):
                var element = new TextNavElement(this);
                break;
            case $(this).is("#searchAttachedFileTag :checkbox"):
                var element = new SearchAttachedFileTagCheckBoxNavElement(this);
                break;
            case $(this).is(":checkbox"):
                var element = new CheckBoxNavElement(this);
                break;
            case $(this).is(".selectBox select"):
                var element = new SelectBoxElement(this);
                break;
            case $(this).is("td.lotsInfo p"):
                var element = new ShowListInfoElement(this);
                break;
            case $(this).is(".tuningIco") && ($.cookie('usePoorVisionOption') == 'true'):
                return;
            case $(this).is(".tuningIco"):
                var element = new TuningIcoElement(this);
                break;
            case $(this).is(".collapceBox"):
                var element = new CollapceBoxElement(this);
                break;
            case $(this).is(".dynatree-expander") && ($(this).find("~ .dynatree-title").length > 0):
                var element = new DynatreeExpanderElement($(this).find("~ .dynatree-title")[0]);
                break;
            case $(this).is(".dynatree-checkbox"):
                var element = new DynatreeCheckboxElement(this);
                break;
            case $(this).is(".disableCollapse .icon-arrow"):
                return;
            case $(this).is(".icon-arrow, .expandLink, div.showDetailDocumentInfo, td.showDetailDocumentInfo"):
                var element = new ReloadedNavElement(this);
                break;
            case $(this).is(".noticeTabBoxWrapper table") && $(this).width() > $(this).closest(".noticeTabBoxWrapper").width():
                var element = new HorizontalScrollableElement(this);
                break;
            case $(this).is("span.downLoad"):
                var element = new LinkElement($(this).closest("a"));
                break;

            default:
                return;
        }
        resultArray.push(element);

    });

    return resultArray;
}


function componentsMenuCollecor() {
    var resultArray = new Array();
    var currPosition = 0;

    $("div.navigation").find("li[class^=nav]:visible").each(function () {
        var element = null;

        switch (true) {
            default:
                element = new OneLineMenuElement(this);
                break;
        }

        if ($(element.item).hasClass("current")) {
            currPosition = resultArray.length;
        }
        resultArray.push(element);
    });
    var result = {
        currentPosition: currPosition,
        navMenuArray: resultArray
    }
    return result;
}

function getPoorvisionSettitngsBlock() {
    var resultArray = new Array();
    $(".poorVisionSettingsBlock").find("*:visible").each(function () {
        switch (true) {
            case $(this).is(".custom-radio label"):
                var element = new PoorVisionControlNavElement(this);
                break;
            case $(this).is(".goodVisionLink"):
                var element = new AbstractNavElement(this);
                break;

            case $(this).is("a"):
                var element = new LinkElement(this);
                break;
            default:
                return;
        }
        resultArray.push(element);

    });

    return resultArray;
}

function startPopUp(currentElementPosition, isKeyboardNavigation){
    $("div.popUp, div.msgBox").off("remove.myEvent",stopPopUp).on("remove.myEvent",stopPopUp);
    stopNavigation();
    mainNavigationTrack = new FocusesElementsMap();
    var popUpElementsFunc = function (){ return componentsCollector("div.popUp, div.msgBox"); };
    mainNavigationTrack.addSection("popUpElements", popUpElementsFunc);
    if (currentElementPosition!=undefined && currentElementPosition !=null){
        mainNavigationTrack.currentPosition.currentSectionName=currentElementPosition.currentSectionName;
        mainNavigationTrack.currentPosition.positionInCurrentSection=currentElementPosition.positionInCurrentSection;
        mainNavigationTrack.getCurrentElement().setFocus();
        return;
    }
    var result=mainNavigationTrack.findElementPositionByItem(clicked_element);
    if (result != null){
        mainNavigationTrack.currentPosition.currentSectionName = result.sectionName;
        mainNavigationTrack.currentPosition.positionInCurrentSection = result.position;
        mainNavigationTrack.next();
    } else {
        mainNavigationTrack.currentPosition.currentSectionName = Object.keys(mainNavigationTrack.navigationBloksMap)[0];
        mainNavigationTrack.currentPosition.positionInCurrentSection = 0;
    }
    if (isKeyboardNavigation) {
        mainNavigationTrack.getCurrentElement().setFocus();
    }
}

function stopPopUp(){
    stopNavigation();
}

function setClickedElement(target){
    clicked_element=$(target);
    if (mainNavigationTrack==null){
        return;
    }

    var result=mainNavigationTrack.findElementPositionByItem(clicked_element);
    if (result != null && result != undefined && result.element === mainNavigationTrack.getCurrentElement()) {
        return;
    }
    if (result != null){
        mainNavigationTrack.getCurrentElement().loseFocus();
        mainNavigationTrack.currentPosition.currentSectionName = result.sectionName;
        mainNavigationTrack.currentPosition.positionInCurrentSection = result.position;
    }
}



$(document).ready(function () {
    $(document).on("click",function(event,param) {
        if (param==1){return ;}
        setClickedElement(event.target);
    });
    $("div.filterParametrs .collapceBox, div.manySelect div.collapsed,div.collapceBox.collapseBoxFooter").on("click",function(event,param) {
        if (param==1){return ;}
        mainNavigationTrack=clearTrack(mainNavigationTrack);
        setClickedElement(event.target);
    });
});

function sortSection(section, rulesName){
    var sectionWithoutChanges = section;
    try {
        var sortedSection = [];
        navigationRules[rulesName].forEach(function (rule) {
            var isFinded = false,
                defaultParent,
                useParentContainer = !(rule['parentContainer'] == undefined || rule['parentContainer'] == ''),
                existDefaultElement = false;
            sortedSection = [];
            section.forEach(function (item) {
                item.viewed = false;
                item.ordered = false;
                for (var j = 0; j < section.length; j++) {
                    if ($(item.item).is(eval(rule.order[j]))){
                        if (!existDefaultElement && rule.default == j){
                            existDefaultElement = true;
                        }
                        item.ordered = true;
                        break;
                    }
                }
            });
            if (!existDefaultElement){
                sortedSection = section;
                return;
            }

            section.forEach(function (item) {
                if (!isFinded && !item.viewed) {
                    if ($(item.item).is(eval(rule.order[rule.default]))) {
                        isFinded = true;
                        defaultParent = useParentContainer ? $(item.item).closest(rule.parentContainer)[0] : '';
                    } else if (!item.ordered){
                        sortedSection.push(item);
                        item.viewed = true;
                    }
                }
                if (isFinded) {
                    for(var s = 0; s < rule.order.length; s++) {
                        for (var j = 0; j < section.length; j++) {
                            var orderItem = section[j];
                            var parent = useParentContainer ? $(orderItem.item).closest(rule.parentContainer)[0] : '';
                            var parentCondition = useParentContainer ? (defaultParent == parent) : true;
                            if ($(orderItem.item).is(eval(rule.order[s])) && !orderItem.viewed && parentCondition){
                                sortedSection.push(orderItem);
                                orderItem.viewed = true;
                                break;
                            }
                        }
                    }
                    isFinded = false;
                }
            });
            section = sortedSection;
        });

        return sortedSection;
    } catch (e){
        return sectionWithoutChanges;
    }
}

function getComplaintsTabs(){
    var allTabs = $("div.childBanner");

    var tempLinkArray = allTabs.toArray();
    var resultArray = [];

    tempLinkArray.forEach(function (item) {
        var elem;
        elem = new AbstractNavElement(item);
        resultArray.push(elem);
    });
    return resultArray;
}






