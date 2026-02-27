
const selectorClassCarousel = ".carousel";
const selectorClassTopLevel = ".topLevel";
const selectorToSpanClassExpandTr = "span.expandTr";
const selectorToTrClassToggleTr = "tr.toggleTr";

$(document).ready(function () {
    if (typeof isRegisteredUserObozApp !== 'undefined' && isRegisteredUserObozApp) {
        sessionMaxInactiveInterval = parseInt(sessionMaxInactiveInterval);
        if (sessionMaxInactiveInterval <= 0) {
            sessionMaxInactiveInterval = 600;
        }
        setTimeout(function () {
            sessionTimeOutMsg();
        }, sessionMaxInactiveInterval * 1000);
    }
    var regionName = Cookies.get('selectElementName');
    if (!regionName) {
        $('.region_name').text('Не выбран');
    } else {
        $('.region_name').text(regionName);
    }
    run_all_scripts();
    initMCustomScrollbar();
    initMaxLengthInputNumber();
    /*observerCommon.observe(document.querySelector("div.listOrg ul"), configObserverAll);*/
});

function scrollUp() {
    var registerBox = $(".registerBox");
    var divScrollUp = $("div.scrollUp");
    if (registerBox.length < 2 && !divScrollUp.hasClass("constant")) {
        divScrollUp.remove();     //Кнопка "Наверх" отображается если количество карточек больше двух
    }

    divScrollUp.click(function () {
        $("html, body").animate({
            scrollTop: 0
        }, "slow");
    });
}

if (!String.prototype.format) {
    String.prototype.format = function () {
        var a = this;
        for (var k in arguments) {
            a = a.replace("{" + k + "}", arguments[k])
        }
        return a
    }
}

function initMCustomScrollbar() {
    $('.select-overflow, .table-overflow').mCustomScrollbar();
}

function destroyMCustomScrollbar() {
    $('.select-overflow, .table-overflow').mCustomScrollbar('destroy');
}
// Для того чтобы работало closest в IE9+
if (!Element.prototype.matches) {
    Element.prototype.matches = Element.prototype.msMatchesSelector ||
        Element.prototype.webkitMatchesSelector;
}

if (!Element.prototype.closest) {
    Element.prototype.closest = function(s) {
        var el = this;

        do {
            if (el.matches(s)) return el;
            el = el.parentElement || el.parentNode;
        } while (el !== null && el.nodeType === 1);
        return null;
    };
}
// Для того чтобы работало includes массивов в IE9+
if (!Array.prototype.includes) {
    Object.defineProperty(Array.prototype, 'includes', {
        value: function(searchElement, fromIndex) {
            if (this == null) {
                throw new TypeError('"this" is null or not defined');
            }
            var o = Object(this);
            var len = o.length >>> 0;
            if (len === 0) {
                return false;
            }
            var n = fromIndex | 0;
            var k = Math.max(n >= 0 ? n : len - Math.abs(n), 0);

            function sameValueZero(x, y) {
                return x === y || (typeof x === 'number' && typeof y === 'number' && isNaN(x) && isNaN(y));
            }
            while (k < len) {
                if (sameValueZero(o[k], searchElement)) {
                    return true;
                }
                k++;
            }
            return false;
        }
    });
}
// Для того чтобы работало CustomEvent в IE9+
(function () {
    if ( typeof window.CustomEvent === "function" ) return false;
    function CustomEvent ( event, params ) {
        params = params || { bubbles: false, cancelable: false, detail: null };
        var evt = document.createEvent( 'CustomEvent' );
        evt.initCustomEvent( event, params.bubbles, params.cancelable, params.detail );
        return evt;
    }
    window.CustomEvent = CustomEvent;
})();
// Для того чтобы работало includes строк в IE9+
if (!String.prototype.includes) {
    String.prototype.includes = function(search, start) {
        'use strict';
        if (typeof start !== 'number') {
            start = 0;
        }

        if (start + search.length > this.length) {
            return false;
        } else {
            return this.indexOf(search, start) !== -1;
        }
    };
}

function expandAllList() {
    $('.expand-all-list .expand-all-list__btn').click(function () {
        var parent = $(this).closest('.expand-all-list');

        var parentForm = $(this).closest('.registry-entry__form');
        parentForm.css('height','auto');

        var itemForHide = $(parent).find('.item-for-hide');
        if ($(this).hasClass('expand-all-list--close')) {
            $(itemForHide).fadeIn();
            $(this).removeClass('expand-all-list--close').text("Скрыть");
        } else {
            $(itemForHide).fadeOut();
            $(this).addClass('expand-all-list--close').text("Показать все("+$(this).attr('data-coutExpandAllList')+")");
        }
    });
}
// Для того чтобы работало maxlength для input[type="number"]
function initMaxLengthInputNumber(){
    $('input[type="number"]').each(function(){
        $(this).on('keyup',function(){
            if ($(this).attr("maxlength") && ($(this).val().length > +($(this).attr("maxlength")))) {
                var val = $(this).val().slice(0, +$(this).attr("maxlength"));
                $(this).val(val);
            }
        });
    });
}

function expandAllListNotCount() {
    $('.js-expand-all-list--not-count .js-expand-all-list__btn').click(function () {
        var parent = $(this).closest('.js-expand-all-list--not-count');
        var itemForHide = $(parent).find('.js-item-for-hide');
        var itemForShow = $(parent).find('.js-item-for-show');
        if ($(this).hasClass('js-expand-all-list--close')) {
            $(itemForHide).fadeIn();
            $(itemForShow).hide();
            $(this).removeClass('js-expand-all-list--close').text("Скрыть");
        } else {
            $(itemForShow).fadeIn();
            $(itemForHide).hide();
            $(this).addClass('js-expand-all-list--close').text("Показать все");
        }
    });
}

var resizeId;
$(window).resize(function () {
    clearTimeout(resizeId);
    resizeId = setTimeout(doneResizing, 500);
});

function doneResizing() {
    var field = $(document.activeElement);
    if (field.is('.hasDatepicker')) {
        field.datepicker('hide').datepicker('show');
    }
}

function _personalCabinetPopUpBlock() {
    const $ddCabinet = $("dd.cabinet");
    const selectorDivPersonalCabinetPopUp = "div.personalCabinetPopUp";
    const selectorDivTuningQuickSearchBlock = "div.tuningQuickSearchBlock";


    $ddCabinet.click(function () {
        $(selectorDivPersonalCabinetPopUp).fadeIn();
    });
    doAfterMouseLeaveCompletely($ddCabinet, 700, function () {
        $(selectorDivPersonalCabinetPopUp).fadeOut(300);
    });
    const selectorDivTuningQuickSearchBox = "div.tuningQuickSearchBox";
    const slideUpDownSize200 = 200;
    const selectorСolonVisible = ':visible';
    $(document).on("click", selectorDivTuningQuickSearchBlock + " .pseudoInput, .pseudoSelect", function () {
        var block = $(this).next(selectorDivTuningQuickSearchBox);
        if (block.is(selectorСolonVisible)) {
            block.slideUp(slideUpDownSize200);
        } else {
            block.slideDown(slideUpDownSize200);
        }
    });

    const selectorDivPseudoSelectClass = "div.pseudoSelect";
    const greyTextClass = "greyText";
    $(document).on("click", "span.tuningQuickSearchTitle.selectItemHandler", function () {
        var item = $(this).data("item");
        $("#questionCommentForm input[type='hidden']").attr("value", $(this).attr("id"));
        var box = $(this).closest(selectorDivTuningQuickSearchBox);
        if (box.is(selectorСolonVisible)) {
            box.slideUp(slideUpDownSize200);
        }
        $(this).closest(selectorDivTuningQuickSearchBlock).find(selectorDivPseudoSelectClass).text($(this).text());
        if (item === '-1') {
            $(this).closest(selectorDivTuningQuickSearchBlock).find(selectorDivPseudoSelectClass).addClass(greyTextClass);
        } else {
            $(this).closest(selectorDivTuningQuickSearchBlock).find(selectorDivPseudoSelectClass).removeClass(greyTextClass);
        }

    });

    const selectedDataItem = "selectedDataItem";
    $(document).on("click", "span#defaultItem.tuningQuickSearchTitle", function () {
        var pseudoSelect = $(this).closest(selectorDivTuningQuickSearchBlock).find(selectorDivPseudoSelectClass);
        pseudoSelect.attr(selectedDataItem, "");
        pseudoSelect.change();
    });

    $(document).on("click", ".tuningQuickSearchTitle.selectItemHandlerExt", function () {
        var item = $(this).data("item");
        var inputName = $(this).data("input-name");
        $("#" + inputName + "SelectOne input[type='hidden']").attr("name", inputName).attr("value", item);
        const greyText9Class = "greyText9";
        if (item === '-1') {
            setTimeout(function () {
                $("#" + inputName + "Select").addClass(greyText9Class);
            }, 20);
            $(this).closest(selectorDivTuningQuickSearchBlock).find(selectorDivPseudoSelectClass).addClass(greyText9Class);
        } else {
            setTimeout(function () {
                $("#" + inputName + "Select").removeClass(greyText9Class);
            }, 20);
            $(this).closest(selectorDivTuningQuickSearchBlock).find(selectorDivPseudoSelectClass).removeClass(greyText9Class);
        }
        var box = $(this).closest(selectorDivTuningQuickSearchBox);
        if (box.is(selectorСolonVisible)) {
            box.slideUp(slideUpDownSize200);
        }
        var pseudoSelect = $(this).closest(selectorDivTuningQuickSearchBlock).find(selectorDivPseudoSelectClass);
        pseudoSelect.text($(this).text());
        pseudoSelect.attr(selectedDataItem, $(this).attr("data-item"));
        pseudoSelect.change();
        $(this).trigger("tuningQuick.selected");
    });

    $(".tuningQuickSearchTitle").click(function () {
        var tittleText = $(this).text();
        $(this).parents(selectorDivTuningQuickSearchBox).prev("div.pseudoInput, .pseudoSelect").removeClass(greyTextClass).text(tittleText).click();
        $(this).trigger("tuningQuick.selected");
        //$("div.filterBlock .bigOrangeBtn").click();
    });


    $(".selectChose .delBtn").click(function () {
        $(this).parent("li").fadeOut(500, function () {
            $(this).remove();
        });
    });
}

function _markerCheckBlock() {
    const selectorSpanMarkerCheck = "div.markerCheck h2 span";
    const selectorDivExpandBox = "div.expandBox";
    const collapseClass = "collapse";
    const selectorСolonChecked = ":checked";
    const $structured = $("#structured");
    const $notStructured = $("#notStructured");
    const slideUpDownSize300 = 300;

    //markerCheck
    $(selectorSpanMarkerCheck).unbind('toggle');

    $structured.click(function () {

        if ($(this).is(selectorСolonChecked) && !$notStructured.is(selectorСolonChecked)) {
            $(selectorSpanMarkerCheck).onSwitch(function () {
                    $(this).addClass(collapseClass);
                    $(this).parent().siblings(selectorDivExpandBox).slideDown(slideUpDownSize300);
                },
                function () {
                    $(this).removeClass(collapseClass);
                    $(this).parent().siblings(selectorDivExpandBox).slideUp(slideUpDownSize300);
                }
            );
        } else {
            $(selectorSpanMarkerCheck).unbind();
        }
    });

    $notStructured.click(function () {
        if (!$(this).is(selectorСolonChecked) && $structured.is(selectorСolonChecked)) {
            $(selectorSpanMarkerCheck).onSwitch(function () {
                    $(this).addClass(collapseClass);
                    $(this).parent().siblings(selectorDivExpandBox).slideDown(slideUpDownSize300);
                },
                function () {
                    $(this).removeClass(collapseClass);
                    $(this).parent().siblings(selectorDivExpandBox).slideUp(slideUpDownSize300);
                }
            );
        } else {
            $(selectorSpanMarkerCheck).unbind();
        }
    });
}

function _choiceLawToAdvancedSearchBlock() {
    const selectorDivLow94Class = "div.low94";
    const selectorDivLow223Class = "div.low223";
    const $inputLow = $("input.low");

    $inputLow.click(function () {
        var index = $inputLow.index(this);
        if (index == 0) {
            $(selectorDivLow94Class + ", " + selectorDivLow223Class).hide();
        }
        if (index == 1) {
            $(selectorDivLow94Class).show();
            $(selectorDivLow223Class).hide();
        }
        if (index == 2) {
            $(selectorDivLow94Class).hide();
            $(selectorDivLow223Class).show();
        }
    });
}

function _alignHeaders() {
    const $hideToggle = $('.revisionToggleWrapper');
    if ($hideToggle) {
        const $heading = $hideToggle.prevAll('.contentHeadingWrapper').first(),
            $mainBox = $('.mainBox'),
            w = $mainBox.width() - $hideToggle.width() - 50;

        $heading.width(w + 'px');
    }
}

function _sizeCarousel(isJQCarousel) {
    const selectorUlCarouselClass = "ul" + selectorClassCarousel;
    const selectorLiCarouselClass = selectorUlCarouselClass + " li";
    const selectorClassJCarousel = (isJQCarousel) ? ".jqcarousel" : ".jcarousel" ;
    const selectorUlJCarouselClass = selectorClassJCarousel + " " + selectorUlCarouselClass;
    const selectorLiJCarouselClass = selectorClassJCarousel + " " + selectorLiCarouselClass;
    const $liJCarousel = $(selectorLiJCarouselClass);

    const parentWidths = $(selectorClassJCarousel).width();
    const $elementCss = (isJQCarousel) ? $liJCarousel : $(this).find(selectorLiCarouselClass) ;
    $elementCss.css("width", parentWidths + "px");

    const childWidths = $liJCarousel.width();
    const childLengths = $liJCarousel.length;
    const ulWidths = childWidths * childLengths;
    $(selectorUlJCarouselClass).css("width", ulWidths + "px");
}

function _sizeLi() {
    _sizeCarousel(false);
}

function _sizeLis() {
    _sizeCarousel(true);
}

function _expandAndCollapseBlock1() {
    const slideUpDownSize300 = 300;
    const selectorTr = "tr";
    const selectorLi = "li";

    $("li.changeMode").click(function () {
        const filterBlockClass = ".filterBlock";
        $(this).parents(filterBlockClass).hide();
        $(this).parents(filterBlockClass).next("div.searchExtendedBlock").fadeIn(500);
    });

    const selectorLiEditLineBtnClass = "li.editLineBtn";
    $(selectorLiEditLineBtnClass).click(function () {
        $(this).hide().siblings("li.editLineField").fadeIn(700);
    });

    $("span.delBtn").click(function () {
        $(this).parent(selectorLi).hide().siblings(selectorLiEditLineBtnClass).fadeIn(700);
    });

    const $divNavigation = $("div.navigation");
    var bodyHeight = $(document).height();
    //$divNavigation.css("height", bodyHeight+"px");

    const selectorTrInternalExpandRowClass = "tr.internalExpandRow";
    const expandViewClass = "expandView";
    $("tr.internalExpandTitle .expandableTitle").onSwitch(function () {
        $(this).addClass(expandViewClass).parent(selectorTr).next(selectorTrInternalExpandRowClass).slideDown(slideUpDownSize300);
    }, function () {
        $(this).removeClass(expandViewClass).parent(selectorTr).next(selectorTrInternalExpandRowClass).slideUp(slideUpDownSize300);
    });

    const usePoorVisionOption = 'usePoorVisionOption';
    const trueText = 'true';
    $divNavigation.click(function () {
        if ($.cookie(usePoorVisionOption) != trueText) {
            expandNavMenu(this);
        }
    });

    doAfterMouseLeaveCompletely($divNavigation, 700, function () {
        if ($.cookie(usePoorVisionOption) != trueText) {
            collapseNavMenu();
        }
    });

    const selectorUlTopLevel = "ul" + selectorClassTopLevel;
    const selectorUlNextLevel = "ul.nextLevel";
    const expandStatusClass = "expandStatus";
    const ul = "ul";
    $(selectorUlTopLevel + " span").click(function () {
        $(this).parents(selectorUlTopLevel).siblings(selectorUlTopLevel).find(selectorLi).removeClass(expandStatusClass);
        $(this).parents(selectorUlTopLevel).siblings(selectorUlNextLevel).slideUp(slideUpDownSize300);
        if ($(this).parent(selectorLi).hasClass(expandStatusClass)) {
            $(this).parents(selectorUlTopLevel).next(selectorUlNextLevel).slideUp(slideUpDownSize300);
            $(this).parent(selectorLi).removeClass(expandStatusClass);
        } else {
            $(this).parents(ul).next(selectorUlNextLevel).slideDown(slideUpDownSize300);
            $(this).parent(selectorLi).addClass(expandStatusClass);
        }
    });

    const selectorUlLastLevelClass = "ul.lastLevel";
    $("li.itemLastLevel").hover(
        function () {
            var ulLastLevel = $(this).addClass(expandStatusClass).find(selectorUlLastLevelClass);
            var ulLastLevelHeight = this.offsetTop + ulLastLevel.height();
            if (ulLastLevelHeight > $(window).height()) {
                var parentHeight = $(this).height();
                var childHeight = ulLastLevel.height();
                if (childHeight > parentHeight) {
                    var top = (childHeight - parentHeight) + 1;
                    ulLastLevel.css("top", "-" + top + "px")
                }
            }
            ulLastLevel.slideDown(slideUpDownSize300);

        }, function () {
            $(this).removeClass(expandStatusClass).find(selectorUlLastLevelClass).slideUp({
                duration: slideUpDownSize300,
                complete: function () {
                    $(this).css("top", "-1px")
                }
            });
        });

    const zero = 0;
    const collapseTrClass = "collapseTr";
    $(selectorToSpanClassExpandTr).onSwitch(
        function () {
            $(this).parents(selectorTr).nextAll(selectorToTrClassToggleTr).eq(zero).slideDown(slideUpDownSize300);
            $(this).addClass(collapseTrClass)
        }, function () {
            $(this).parents(selectorTr).nextAll(selectorToTrClassToggleTr).eq(zero).slideUp(slideUpDownSize300);
            $(this).removeClass(collapseTrClass)
        }
    );

    const rl90Class = "rl-90";
    $(selectorToSpanClassExpandTr + "-right").onSwitch(
        function () {
            $(this).parents(selectorTr).nextAll(selectorToTrClassToggleTr).eq(zero).slideDown(slideUpDownSize300);
            $(this).removeClass(rl90Class);
            $(this).addClass(collapseTrClass);
        }, function () {
            $(this).parents(selectorTr).nextAll(selectorToTrClassToggleTr).eq(zero).slideUp(slideUpDownSize300);
            $(this).addClass(rl90Class);
            $(this).removeClass(collapseTrClass);
        }
    );

    $(document).on("click", "li.pageSelect span", function () {
        const openListClass = "openList";
        const slideUpDownSize200 = 200;
        if ($(this).hasClass(openListClass)) {
            $(this).next(ul).slideUp(slideUpDownSize200);
            $(this).removeClass(openListClass);
        } else {
            $(this).addClass(openListClass);
            $(this).next(ul).slideDown(slideUpDownSize200);
        }
    });
}

function _expandAndCollapseBlock2() {
    const expandClass = "expand";
    const expandBoxClass = expandClass + "Box";
    const selectorExpandClass = "." + expandClass;
    const selectorDivExpandBox = "div." + expandBoxClass;
    const selectorDiv = "div";
    const selectorCollapceClass = ".collapce";

    $("div.collapceBox").click(function () {
        var $this = $(this);
        $this.next(selectorDiv).slideToggle(function () {
            $this.toggleClass(expandBoxClass);
            const epzPath = "/epz";
            if ($this.id && !$.cookie($this.id)) {
                $.cookie($this.id, "false", {path: epzPath});
            } else {
                $.removeCookie($this.id, {path: epzPath});
            }
        });
    });

    $("div.collapceAdd").click(function () {
        var $this = $(this);
        $this.parent(selectorDiv).next(selectorDiv).slideToggle(function () {
            $this.toggleClass(expandBoxClass);
            const dNoneClass = "d-none";
            if ($this.hasClass(expandBoxClass)) {
                $this.children(selectorExpandClass).toggleClass(dNoneClass);
                $this.children(selectorCollapceClass).toggleClass(dNoneClass);
            } else {
                $this.children(selectorExpandClass).toggleClass(dNoneClass);
                $this.children(selectorCollapceClass).toggleClass(dNoneClass);
            }
        });
    });

    const slideUpDownSize300 = 300;
    const selectorDivCollapseBlockClass = "div.collapseBlock";
    $("div.collapceBase").onSwitch(function () {
            $(this).parent(selectorDivCollapseBlockClass).next(selectorDiv).slideUp(slideUpDownSize300);
            $(this).addClass(expandBoxClass);
        },
        function () {
            $(this).parent(selectorDivCollapseBlockClass).next(selectorDiv).slideDown();
            $(this).removeClass(expandBoxClass);
        });

    $(document).on('click', 'div.collapse__title', function () {
        var $this = $(this);
        $this.toggleClass("collapse__title_opened");
        $this.siblings("div.collapse__content").slideToggle("slow");
    });

    set_collapsed_state();

    const slideUpDownSize100 = 100;
    $("ul.leftColAskList > li").not('.disableCollapse').click(
        function () {
            var rubricItem = $(this);
            const collapceArrowClass = "collapceArrow";
            const ul = "ul";
            if (rubricItem.hasClass(collapceArrowClass)) {
                rubricItem.find(ul).slideUp(slideUpDownSize100);
                rubricItem.removeClass(collapceArrowClass);
            } else {
                rubricItem.find(ul).slideDown(slideUpDownSize100);
                rubricItem.addClass(collapceArrowClass);
            }
        });


    const selectorClassCommentWrapper = ".commentWrapper";
    const margBtm40Class = "margBtm40";
    const answereBlockClass = ".answereBlock";
    const margTop0Class = "margTop0";
    const selectorTitleCommentClass = ".titleComment";
    const selectorDivTitleCommentClass = selectorDiv + selectorTitleCommentClass;
    $("div.askBox " + selectorTitleCommentClass).onSwitch(
        function () {
            $(this).next(selectorClassCommentWrapper).addClass(margBtm40Class);
            $(this).next(selectorClassCommentWrapper).find(answereBlockClass).slideDown(slideUpDownSize100);
            $(this).next(selectorClassCommentWrapper).next(selectorDivTitleCommentClass).removeClass(margTop0Class);
            $(this).addClass(expandClass);
        },
        function () {
            $(this).next(selectorClassCommentWrapper).removeClass(margBtm40Class);
            $(this).next(selectorClassCommentWrapper).find(answereBlockClass).slideUp(slideUpDownSize100);
            $(this).removeClass(expandClass);
            $(this).next(selectorClassCommentWrapper).next(selectorDivTitleCommentClass).addClass(margTop0Class);
        });

    $(".questionLink").click(function () {
        if ($(this).next(answereBlockClass).is(":hidden")) {
            $(this).parent(selectorClassCommentWrapper).prev(selectorTitleCommentClass).click();
            $(this).next(answereBlockClass).slideDown(slideUpDownSize100);
            $(this).parent(selectorClassCommentWrapper).addClass(margBtm40Class);
            $(this).parent(selectorClassCommentWrapper).next(selectorDivTitleCommentClass).removeClass(margTop0Class);
            $(this).parent(selectorClassCommentWrapper).prev(selectorTitleCommentClass).addClass(expandClass);
        } else {
            $(this).parent(selectorClassCommentWrapper).prev(selectorTitleCommentClass).click();
            $(this).next(answereBlockClass).slideUp(slideUpDownSize100);
            $(this).parent(selectorClassCommentWrapper).removeClass(margBtm40Class);
            $(this).parent(selectorClassCommentWrapper).next(selectorDivTitleCommentClass).addClass(margTop0Class);
            $(this).parent(selectorClassCommentWrapper).prev(selectorTitleCommentClass).removeClass(expandClass);
        }
    });

    const collapseClass = "collapse";
    const spanSelector = "span";
    $("div.expCol h1").onSwitch(function () {
            $(this).removeClass(expandClass).addClass(collapseClass).find(spanSelector).text("Свернуть меню");
            $("div.expandMenu").fadeIn(2000);
            $("div.collapseMenu").hide();
        },
        function () {
            $(this).removeClass(collapseClass).addClass(expandClass).find(spanSelector).text("Развернуть меню");
            $("div.collapseMenu").fadeIn(2000);
            $("div.expandMenu").hide();
        }
    );

    const zero = 0;
    $("div.expandBlock h2 span").onSwitch(function () {
            $(this).addClass(collapseClass);
            $(this).parent().siblings(selectorDivExpandBox).slideDown(slideUpDownSize300);
            $(this).parent().trigger("onExpand");
        },
        function () {
            $(this).removeClass(collapseClass);
            $(this).parent().siblings(selectorDivExpandBox).slideUp(slideUpDownSize300);
            $(this).parent().siblings(selectorDivExpandBox).delay(zero).hide(zero);
        }
    );

    const selectorDivQuestionBoxClass = "div.questionBox";
    const selectorCollapseBtnClass = ".collapseBtn";
    $(selectorDivQuestionBoxClass + " .question span.questionText").onSwitch(function () {
            $(this).parent().siblings(selectorCollapseBtnClass).addClass(collapseClass);
            $(this).parents(selectorDivQuestionBoxClass).siblings(selectorDivExpandBox).slideDown(slideUpDownSize300);
        },
        function () {
            $(this).parent().siblings(selectorCollapseBtnClass).removeClass(collapseClass);
            $(this).parents(selectorDivQuestionBoxClass).siblings(selectorDivExpandBox).slideUp(slideUpDownSize300);
        }
    );

    const selectorDivQuestionClass = "div.question";
    const selectorSpanQuestionTextClass = "span.questionText";
    $("div.questionBox .collapseBtn").onSwitch(function () {
            $(this).addClass(collapseClass);
            $(this).siblings(selectorDivQuestionClass).find(selectorSpanQuestionTextClass).click();
        },
        function () {
            $(this).removeClass(collapseClass);
            $(this).siblings(selectorDivQuestionClass).find(selectorSpanQuestionTextClass).click();
        }
    );


    const collapsedClass = "collapsed";
    const selectorAnswerClass = ".answer";
    const selectorBodyClass = ".body";
    $("div.purchaseComments .question span").onSwitch(function () {
            $(this).addClass(collapsedClass);
            $(this).parent().parent().removeClass(collapsedClass);
            $(this).parent().siblings(selectorBodyClass).slideDown(slideUpDownSize300);
            $(this).parent().parent().siblings(selectorAnswerClass).slideDown(slideUpDownSize300);
        },
        function () {
            $(this).removeClass(collapsedClass);
            $(this).parent().parent().addClass(collapsedClass);
            $(this).parent().siblings(selectorBodyClass).slideUp(slideUpDownSize300);
            $(this).parent().parent().siblings(selectorAnswerClass).hide(zero);
        }
    );
}

function _pollBlock() {
    const textAreaPoll = $("dl.pollDown textarea");
    const describeYourOpinionText = "Опишите свое мнение";
    const colorValueClass = "colorValue";
    const emptyText = "";

    textAreaPoll.val(describeYourOpinionText);
    textAreaPoll.focus(function () {
        if ($(this).val() === describeYourOpinionText) {
            $(this).val(emptyText).removeClass(colorValueClass);
        }
        ;
    });
    textAreaPoll.blur(function () {
        if ($(this).val() === emptyText) {
            $(this).val(describeYourOpinionText).addClass(colorValueClass);
        }
    });
}

function _resizeBlock() {
    $(window).resize(function () {
        _sizeLi();
        var eL = $(".jcarousel div.pagingList ul li.active");
        var index = $(eL).index();
        const $mycarousel = $("#mycarousel ul" + selectorClassCarousel);
        var liWidth = $mycarousel.find("li").width();
        var posUl = index * liWidth;
        $mycarousel.css("left", -posUl + "px");
    });

    $(window).resize(function () {
        sizeLiOther();
        var eL = $("div.topClients:visible .pagingList ul li.active");
        var index = $(eL).index() - 1;
        var liWidth = $("div.topClients:visible " + selectorClassCarousel + " li").width();
        var posUl = index * liWidth;
        $("div.topClients:visible ul" + selectorClassCarousel).css("left", -posUl + "px");
    });

    //$(window).resize(function(){
    //        sizeLiOther();
    //        $("div.carouselInfo .pagingList ul" + selectorClassCarousel).each(function(){
    //        var index = $(this).find("li.active").index()-1;
    //        var liWidth = $("div.topClients:visible " + selectorClassCarousel + " li").width();
    //         var posUl = index*liWidth;
    //        $(this).css("left",-posUl+"px");
    //        });
    //});
}

function run_all_scripts() {

    // $(".manySelect .collapsed").on("click",function () {
    //     select();
    // });
    $('[data-close]').css('cursor', 'pointer');
    $(document).mouseup(function (e) {
        if ($('.modal-content').has(e.target).length === 0) {
            if ($(e.target).hasClass("modal")) {
                $('div.popupModalOverlay').remove();
            }
        }
    });
    $(".showDetailDocumentInfo").on("click", function () {
        showDetailDocumentInfo(this);
    });

    $(".showDetailDocumentInfoWithArrow").on("click", function () {
        showDetailDocumentInfo(this);
        $(this).toggleClass("collapceArrow");
    });

    $(".showInactiveDocumentEdition").on("click", function () {
        showInactiveDocumentEdition(this);
    });
//Вкладки
    var tabsB = "ul.catalogTabs li";
    var tabsP = "table.purchaseTabs td";

    _alignHeaders();

    //Очистка инпута
    $("input.clearValue").keyup(function () {
        if (this.value != '') {
            $(this).next("span.clearButton").css("visibility", "visible");
        } else {
            $(this).next("span.clearButton").css("visibility", "hidden");
        }
    });

    $("div .clearButton").click(function () {
        $(this).parent().find("input.clearValue").val("");
        $(this).parent().find("input.clearValue").focus();
        $(this).css("visibility", "hidden");
    });

    //

    var leftColHeight = $("div.leftCol").height();
    var rightColHeight = $("div.rightCol").height();
    $("div.leftCol > ul").css("min-height", rightColHeight + "px");

    $("table.purchaseTabs td").click(function () {
        var tabsP = "table.purchaseTabs td";
        var eL = this;
        var index = $(tabsP).index(eL);
        $(eL).siblings("td").removeClass("currentTab");
        $(eL).addClass("currentTab");
        $(eL).parents("table").next(".tabsBox").children("div").css("display", "none");
        $(eL).parents("table").next(".tabsBox").children("div").eq(index).css("display", "block");
    });

    $(".orangeTabs td").click(function () {
        var tabsP = "div.orangeTabs td";
        var eL = this;
        var index = $(tabsP).index(eL);
        $(eL).siblings("td").removeClass("currentTab");
        $(eL).addClass("currentTab");
        $(eL).parents("div").next(".tabsBox").children("div").css("display", "none");
        $(eL).parents("div").next(".tabsBox").children("div").eq(index).css("display", "block");
    });


    var altText = $("div.btnLinePopUp input.bigOrangeBtn").attr("alt");
    $("div.btnLinePopUp input.bigOrangeBtn.addTitle").attr("title", altText);


    $("tr.tdHead :checkbox").click(function () {
        if ($(this).is(":checked")) {
            $(this).parents("table").siblings("div.boxSelectedTabs").find("div.visibleDiv").find("input[type='checkbox']").attr("checked", "checked");
            $("div.btnLinePopUp input.bigOrangeBtn").removeAttr("title").removeClass("disabledBtn").removeClass("addTitle");
            $(".tabsTblParam").find("td.parasmetersAction ").find("td").eq(0).show();
        } else {
            $(this).parents("table").siblings("div.boxSelectedTabs").find("div.visibleDiv").find("input[type='checkbox']").removeAttr("checked");
            var altText = $("div.btnLinePopUp input.bigOrangeBtn").attr("alt");
            $("div.btnLinePopUp").find(".bigOrangeBtn").addClass("addTitle").addClass("disabledBtn").attr("title", altText);
            $(".tabsTblParam").find("td.parasmetersAction ").find("td").eq(0).hide();
        }
    });

    setupPollMenu();

    $("ul.tabsStatus li").click(function () {
        var el = this;
        var index = $("ul.tabsStatus li").index(el);
        $(el).siblings("li").removeClass("currentStatus");
        $(el).addClass("currentStatus");
        $(el).parents("ul").siblings("div.tabsBlock").css("display", "none");
        $(el).parents("ul").siblings("div.tabsBlock").eq(index).css("display", "block");
    });

    $("#expandLotsInfo").click(function () {
        expandLots($(this));
    });

    $("td.lotsInfo p").click(function () {
        var $container = $(this).closest("td.lotsInfo");
        $($container).children("p").first().toggleClass("expandP");
        if ($container.find(".expandP").length > 0) {
            $($container).children("div").first().slideDown(300);
        } else {
            $($container).children("div").first().slideUp(300);
        }
    });

    $(".lowFz94").click(function () {
        if ($(this).is(":checked")) {
            $("tr.fz94hide").css("display", "none");
            $("tr.fz94show").css("display", "table-row");
        }
    });

    $(".lowFz94Fz223, .lowFz223").click(function () {
        if ($(this).is(":checked")) {
            $("tr.fz94show").css("display", "none");
            $("tr.fz94hide").css("display", "table-row");
        }
    });

    checkSwitcher();
    lowChoice();

    $("ul.listTop li").click(function () {
        var indexLi = $(this).index();
        $(this).siblings("li").removeClass("selectPos");
        $(this).addClass("selectPos");
        $(this).parent("ul.listTop").siblings("div.carouselInfo").find("table.listTopClients").find("tr").find("td.changeTd").hide();
        $(this).parent("ul.listTop").siblings("div.carouselInfo").find("table.listTopClients").find("tr").each(function () {
            $(this).find("td").eq(indexLi + 2).show();
        });
    });

    $("table.ratingLots .listTop li").click(function () {
        var indexLi = $(this).index();
        $(this).parents("td").siblings("td").find("p").hide();
        $(this).parents("td").siblings("td").find("p").eq(indexLi).show();
        $(this).parents("table").siblings("table.listTopClients").hide();
        $(this).parents("table").siblings("table.listTopClients").eq(indexLi).show();
    });

    diagramBox();

    preparePopupDialogLinks("a.linkPopUp");
    prepareNewExternalPopupDialogLinks("a.newExternalPopUpLink");
    prepareExternalSignLinks();

    $("a.addBtn").click(function (event) {
        $("div.popUp").addClass("okdpPopUp");
    });

    $("a.eds").click(function (event) {
        $("div.popUp").addClass("ElDigSign");
    });

    $("body").on("click", "div.overlay, div.popupOverlay, .closePopUp, .btn-close, div.edsContainer .btn", function () {
        //$("div.overlay, div.popUp, div.popupOverlay").remove();
        //$("body").removeClass("openmodal");
        $("div.overlay, div.popUp, div.popupModalOverlay, div.popupOverlay").remove();
    });

    $("li.sortItem").click(function () {
        //$(this).addClass("activeSort").siblings("li").removeClass("activeSort");
        if ($(this).hasClass("activeSort")) {
            if ($(this).hasClass("arrowTop")) {
                $(this).removeClass("arrowTop");
            } else {
                $(this).addClass("arrowTop");
            }
        }
        $(this).addClass("activeSort").siblings("li").removeClass("activeSort");
        $("[name=sortBy]").val($("li.activeSort").attr("type"));
        $("[name=sortDirection]").val($("li.activeSort").hasClass("arrowTop") ? 'true' : 'false');
        submitQuickSearchForm()

    });

    // $(window).scroll(function () {
    //     var popUpScroll = $(document).scrollTop();
    //     var windowHeight = $(window).height();
    //     var popUpHeight = $("div.popUpWrapper").innerHeight();
    //     var poorVisionHeight = $('.poorVision .poorVisionSettingsBlockDiv2').height();
    //
    //     if (($.cookie('usePoorVisionOption') == 'true') && popUpHeight > windowHeight - (poorVisionHeight * 2)) {
    //         $("div.popUp").css("margin-top", popUpScroll + "px");
    //         $("div.popUp").css("top", poorVisionHeight + "px");
    //     } else if (popUpHeight > windowHeight) {
    //         $("div.popUp").css("margin-top", popUpScroll + "px");
    //         $("div.popUp").css("top", "0px");
    //     } else {
    //         $("div.popUp").css("margin-top", popUpScroll - popUpHeight / 2 + "px");
    //     }
    // });

    $(window).resize(function () {
        resizePopupDialog();
    });

    var registerBox = $(".registerBox");
    var divScrollUp = $("div.scrollUp");
    if (registerBox.length < 2 && !divScrollUp.hasClass("constant")) {
        divScrollUp.remove();     //Кнопка "Наверх" отображается если количество карточек больше двух
    }

    divScrollUp.click(function () {
        $("html, body").animate({
            scrollTop: 0
        }, "slow");
    });

//Личный кабинет popUp
    _personalCabinetPopUpBlock();


//expand/collapce
    _expandAndCollapseBlock1();

    $(document).on("click", "li.pageSelect li", function () {
        var changeData = $(this).text();
        $(this).parents("ul").siblings("span").find("i").text(changeData).click();
        $("[name=recordsPerPage]").val($(this).attr("id"));
        if (typeof submitQuickSearchForm == 'function') {
            submitQuickSearchForm(true);
        } else if (typeof submitForm == 'function') {
            submitForm(1, changeData);
        }
    });

    _expandAndCollapseBlock2();

    _markerCheckBlock();

    collapseInitialize();

    manySelect();

    $("table.lowSearch>tbody>tr>td:nth-child(1)").addClass("firstTd");
    $("table.versionLogList tr:nth-child(odd)").addClass("colorTR");


//close btn
    listOrgRemove();

//фокус input, textarea
//    $("input.hint:text").focus(function () {
//        if (this.value === this.title) {
//            $(this).attr("value", "").removeClass("colorValue");
//        }
//    }).blur(function () {
//        if (this.value === this.title || this.value == '') {
//            $(this).attr("value", this.title).addClass("colorValue");
//        }
//    });

    setupHint(".lowSearch input[type='text']");

    _pollBlock();

// ---- datePicker -----
    $(".old-search-form .datePicker").each(function (index, element) {
        $(element).datepick({
            rangeSelect: false
        });
    });

    $(function () {
// ---- autocomplete -----
        var availableTags = ["1", "12", "123", "1234", "12345", "1234", "234", "2345", "345", "3456"];
        $('input.autocompleteOrder:not(.withoutAutocomplete)').autocomplete({
            source: availableTags,
            minLength: 1
        });
    });

    jQuery.ui.autocomplete.prototype._resizeMenu = function () {
        var ul = this.menu.element;
        ul.outerWidth(this.element.outerWidth());
    }

// выбор закона на срранице расширенного поиска
    _choiceLawToAdvancedSearchBlock();

    _resizeBlock();

    $(_sizeLi);
    $(_sizeLis)
    $(sizeLiOther);
    $(carouselWithoutArrow);
    $(carouselOther);


    // $("#slider").slider({
    //     range: true,
    //     min: 0,
    //     max: 2000000000,
    //     values: [0, 2000000000],
    //     slide: function (event, ui) {
    //         $("#minPrice").val(ui.values[0]);
    //         $("#maxPrice").val(ui.values[1]);
    //         Mask();
    //     },
    //
    //     change: function (event, ui) {
    //         Mask();
    //     }
    // });
    // $("#amountmin").val($("#slider").slider("values", 0));
    // $("#amountmax").val($("#slider").slider("values", 1));
    // $("#minPrice").val($("#slider").slider("values", 0));
    // $("#maxPrice").val($("#slider").slider("values", 1));

    /*$("#minPrice").blur(
     function(event) {
     if ($("#slider").slider("values", 1) < deleteSpaces($(this).val())) {
     $(this).val($("#slider").slider("values", 1));
     }
     $("#slider").slider("values", 0, deleteSpaces(this.value));
     });
     $("#maxPrice").blur(
     function(event) {
     if ($("#slider").slider("values", 0) > deleteSpaces($(this).val())) {
     $(this).val($("#slider").slider("values", 0));
     }
     if (deleteSpaces($("#amountmax").val()) < deleteSpaces($(this).val())) {
     $(this).val(deleteSpaces($("#amountmax").val()));
     }
     $("#slider").slider("values", 1, deleteSpaces(this.value));
     });*/
    $(Mask);

    function deleteSpaces(str) {
        var RegEx = /\s/g;
        return parseInt(str.replace(RegEx, ""));
    }

    function Mask() {
        $(".mask").priceFormat({
            prefix: '',
            centsSeparator: '',
            thousandsSeparator: ' '
        });
    };

    customSelect();
}

String.prototype.htmlEscape = function () {
    return $('<div/>').text(this.toString()).html();
};

//Custom select
function customSelect() {
    jQuery('select').not(".chosenSelect").each(function () {
        if (jQuery(this).next().length > 0 && jQuery(this).next().hasClass('select')) return;
        var title = jQuery(this).attr('title');
        if (jQuery('option:selected', this).val() != '') title = jQuery('option:selected', this).text().htmlEscape();
        jQuery(this)
            .css({'z-index': 10, 'opacity': 0, '-khtml-appearance': 'none', 'margin-left': '1px'})
            .after('<span class="select"><span class="selectRight"></span>' + title + '</span>')
            .change(function () {
                val = jQuery('option:selected', this).text().htmlEscape();
                if (!jQuery(this).hasClass("withoutTitle")) jQuery(this).attr("title", val);
                val = "<span class='selectRight'></span>" + val;
                jQuery(this).next().html(val);
                markPlaceholder(this);
                $(document).trigger('title-attribute-changed');
            });
        if (!jQuery(this).hasClass("withoutTitle")) jQuery(this).attr("title", title);
        markPlaceholder(this);
    });

    function markPlaceholder(select) {
        var val = jQuery('option:selected', select).text().htmlEscape();
        jQuery(select).next().toggleClass('colorValue', val === $(select).data('placeholder'));
    }
}

//Creates custom selects for selects inside section with provided id - parentId;
function applyCustomSelect(parentId) {

    if (!parentId) {
        customSelect();
        return;
    }

    $('#' + parentId).find('select').not(".chosenSelect").each(function () {
        var title = jQuery(this).attr('title');
        if (jQuery('option:selected', this).val() != '') title = jQuery('option:selected', this).text().htmlEscape();
        jQuery(this)
            .css({'z-index': 10, 'opacity': 0, '-khtml-appearance': 'none', 'margin-left': '1px'})
            .after('<span class="select"><span class="selectRight"></span>' + title + '</span>')
            .change(function () {
                val = jQuery('option:selected', this).text().htmlEscape();
                jQuery(this).attr("title", val);
                val = "<span class='selectRight'></span>" + val;
                jQuery(this).next().html(val);
            })
        jQuery(this).attr("title", title);
    });
}

// carousel other
function carouselOther() {
    const selectorLeftArrowClass = ".leftArrow";
    const selectorRightArrowClass = ".rightArrow";
    const selectorSpanLeftArrowClass = selectorLeftArrowClass + " span";
    const selectorSpanRightArrowClass = selectorRightArrowClass + " span";
    const selectorToDivClassPagingList = "div.pagingList";
    $("div.carouselInfo .pagingList ul li").not(selectorLeftArrowClass + ", " + selectorRightArrowClass).click(
        function () {
            var eL = this;
            var index = $(this).index() - 1;
            const selectorUlCarouselClass = "ul" + selectorClassCarousel;
            var liWidth = $(this).parents(selectorToDivClassPagingList).siblings(selectorUlCarouselClass).find("li").width();
            var liSize = $(this).parents(selectorToDivClassPagingList).siblings(selectorUlCarouselClass).find("li").size();
            $(eL).siblings("li").removeClass("active");
            $(eL).addClass("active");
            var posUl = index * liWidth;
            $(this).parents(selectorToDivClassPagingList).siblings(selectorUlCarouselClass).animate({left: -posUl}, 500);
            const disableArrowClass = "disableArrow";
            $(selectorSpanLeftArrowClass).removeClass(disableArrowClass);
            $(selectorSpanRightArrowClass).removeClass(disableArrowClass);
            if (index == 0) {
                $(selectorSpanLeftArrowClass).addClass(disableArrowClass);
            }
            if (index == liSize - 1) {
                $(selectorSpanRightArrowClass).addClass(disableArrowClass);
            }
        });
}

function carouselWithoutArrow() {
    $("div.carouselInfo .withoutArrow ul li").not(".leftArrow, .rightArrow").click(
        function () {
            const eL = this;
            const index = $(this).index();
            const selectorToDivClassWithoutArrow = "div.withoutArrow";
            const liWidth = $(this).parents(selectorToDivClassWithoutArrow).siblings("ul" + selectorClassCarousel).find("li").width();
            const liSize = $(this).parents(selectorToDivClassWithoutArrow).siblings("ul" + selectorClassCarousel).find("li").size();
            $(eL).siblings("li").removeClass("active");
            $(eL).addClass("active");
            const posUl = index * liWidth;
            $(this).parents(selectorToDivClassWithoutArrow).siblings("ul" + selectorClassCarousel).animate({left: -posUl}, 500);
        });
}

function sizeLiOther() {
    var parentWidthOther = $("div.purchaseBox .tabsBox>div:visible").width();
    $("div.carouselInfo " + selectorClassCarousel + " li").css("width", parentWidthOther + "px");
    var childWidthOther = $("div.carouselInfo " + selectorClassCarousel + " li").width();
    var childLengthOther = $("div.carouselInfo " + selectorClassCarousel + " li").length;
    var ulWidthOther = childWidthOther * childLengthOther;
    $("div.carouselInfo " + selectorClassCarousel).css("width", ulWidthOther + "px");
}


function lowChoice() {
    $("ul.lowChoice li").click(function () {
        var indexLi = $(this).index();
        $(this).parent("ul.lowChoice").siblings("div").css("visibility", "hidden");
        $(this).parent("ul.lowChoice").siblings("div").eq(indexLi).css("visibility", "visible");
    });
}

//switchers on Info about
function checkSwitcher() {
    $("span.switcher").click(function () {
        var indexSwitcher = $(this).index();
        $(this).siblings("span").removeClass("checkSwitcher");
        $(this).addClass("checkSwitcher");
        $(this).parent(".switcherBox").siblings("dl").find("dd").hide();
        $(this).parent(".switcherBox").siblings("dl").find("dd").eq(indexSwitcher).show();
    });
}

function diagramBox() {
    $("div.diagramBox span").click(function () {
        var indexSpan = $(this).index();
        $(this).siblings("span").removeClass("active");
        $(this).addClass("active");
        $(this).siblings("div").hide();
        $(this).siblings("div").eq(indexSpan).show();
    });
}

function manySelect() {
    $(".manySelect .collapsed").click(function () {
        if ($(this).parent().find("li").length > 0 || $(this).parent().find("div").length > 0) {
            $(this).find("span.msExpandButton").toggleClass("active");
            $(this).siblings("div.selectChose").slideToggle("slow", function () {
            });
        }
    });

    $(document).on('click', '.manySelect .selectChose ul li, .manySelect .selectChose ul div', function () {
        var input = $(this).find("input[type=checkbox]");
        var checked = input.prop("checked");
        if ((typeof input) !== 'undefined' && (typeof checked) !== 'undefined') {
            input.data("checked", !checked);
            input.prop("checked", !checked);
        }
    });

    $("div.msButtonIn span.btnBtn").click(function () {
        var checkedInputs = $(this).parent().siblings("div.expanded").find("input:checked:enabled").not(".not-selected-many-select-item");
        var inputLabel = _buildInputLabel(checkedInputs);
        var placeholderText = $(this).parents(".selectChose").siblings(".collapsed").find(".msPlaceholder").data("initial");
        $(this).parents(".manySelect").find("span.msExpandButton").toggleClass("active");
        $(this).parents("div.selectChose").slideToggle("slow", function () {
            //$(this).find("span.msExpandButton").toggleClass("active");
        });
        if (inputLabel == '') {
            $(this).parents(".selectChose").siblings(".collapsed").find(".msPlaceholder").text(placeholderText).removeClass("choseColor")
        } else {
            $(this).parents(".selectChose").siblings(".collapsed").find(".msPlaceholder").text(inputLabel).addClass("choseColor");
        }
    });
}

function _buildInputLabel(checkedInputs) {
    var inputLabel = checkedInputs.parent("li").map(
        function () {
            return $(this).text();
        }).get().join(", ");
    if (!inputLabel) {
        var valueList = checkedInputs.parent().parent("li").map(
            function () {
                var customValueElem = $("input[type='text']", this);
                if (customValueElem.length == 1) {
                    var title = customValueElem.attr("title");
                    var val = customValueElem.val();
                    if (val == title) {
                        val = '';
                    }
                    return val;
                }
                return $(this).text();
            }).get();
        valueList = $.grep(valueList, function (n, i) {
            return n != '';
        });
        inputLabel = valueList.join(", ");
    }
    return inputLabel;
}

function listOrgRemove(afterRemoveCallback) {
    $("div.listOrg ul li span:not(.noBgImg)").filter(function () {
        return !$(this).hasClass("removeDrugEl");
    }).click(function () {
        //$(this).parent("li").fadeOut(500, function(){
        var container = $(this).closest('ul');
        $(this).closest("li").remove();
        if (afterRemoveCallback) {
            afterRemoveCallback();
        }
        //});
        // var lengthLi = $(this).parents("div.listOrg").find("li:visible").length;
        // if (lengthLi === 1) {
        //     $(this).parents("div.listOrg").children("p").find("input[type='checkbox']").attr("checked", false);
        // }

        var containerChildren = container.children();
        var containerChildrenCount = containerChildren.length;
        var containerDropdownListTitle = container.find('a');
        if (containerChildrenCount >= 5) {
            if (containerDropdownListTitle.hasClass("show")) {
                container.find('.displayNone:first').removeClass().addClass('upperCase').removeAttr('style');
                containerDropdownListTitle.text('Выбрано ещё ' + container.find("li:hidden").length);
            } else {
                if (container.find('li:not(.displayNone)').length < 4) {
                    container.find('.displayNone:first').removeClass().addClass('upperCase').removeAttr('style');
                }
            }
            containerDropdownListTitle.attr('count', +containerDropdownListTitle.attr('count') - 1);
            if (containerChildrenCount === 5) containerDropdownListTitle.hide();
        }
    });

    /*$("span.deleteDoc").click(function () {
     $(this).parents("tr").remove();

     });*/
}

function getIdArray(divId) {
    var elems = $("div#" + divId + " ul li:visible");
    var arr = jQuery.makeArray(elems);
    return $.map(arr, function (val, i) {
        return $(val).attr('id');
    });
}

function collapseInitialize() {
    $("h1.headerPanel.collapse").onSwitch(function () {
            $(this).removeClass("collapse").addClass("expand");
            $(this).next("div.tableEditor").slideDown(300);
        },
        function () {
            $(this).removeClass("expand").addClass("collapse");
            $(this).next("div.tableEditor").slideUp(300);
        });


    $("span.editDoc").click(function () {
        $("h1.headerPanel.collapse").click();
        $("div.tableEditor").slideDown(300);
    });
}

function showFirst5Comments() {
    var additionalTableHeight = 0;
    for (var i = 0; i < 5; i++) {
        if ($('#commentTd' + i) != null) {
            var commentLength = $('#spanComment' + i).text().length;
            additionalTableHeight = additionalTableHeight + (commentLength / 130 + 1) * 15;
        }
    }
    additionalTableHeight = (additionalTableHeight + 100);
    $('#commentsTable').css('height', additionalTableHeight + 'px');
}

function setupPollMenu() {
    $("ul.tabPollMenu li").click(function () {
        var el = this;
        var index = $("ul.tabPollMenu li").index(el);
        $(el).siblings("li").removeClass("currentPoll");
        $(el).addClass("currentPoll");
        $(el).parents("ul").siblings("div.pollBlock").css("display", "none");
        $(el).parents("ul").siblings("div.pollBlock").eq(index).css("display", "block");
    });
}

function setupHint(selector) {
    $(selector).focus(function () {
        var valueText = $(this).val()
        if (this.title == valueText) {
            this.value = ''
        }
        ;
        $(this).removeClass("colorValue");
    });

    $(selector).not(".datepicker_ru").blur(function () {
        var valueText = $(this).data("initial");
        if (this.value == '' && !(typeof valueText === 'undefined')) {
            this.value = valueText;
            $(this).addClass("colorValue");
        }
    });
}

function createCookie(name, value, days) {
    if (days) {
        var date = new Date();
        date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
        var expires = "; expires=" + date.toGMTString();
    } else var expires = "";
    document.cookie = name + "=" + value + expires + "; path=/";
}

function readCookie(name) {
    var nameEQ = name + "=";
    var ca = document.cookie.split(';');
    for (var i = 0; i < ca.length; i++) {
        var c = ca[i];
        while (c.charAt(0) == ' ') c = c.substring(1, c.length);
        if (c.indexOf(nameEQ) == 0) return c.substring(nameEQ.length, c.length);
    }
    return null;
}

function eraseCookie(name) {
    createCookie(name, "", -1);
}

function createUUID() {
    var s = [];
    var hexDigits = "0123456789abcdef";
    for (var i = 0; i < 36; i++) {
        s[i] = hexDigits.substr(Math.floor(Math.random() * 0x10), 1);
    }
    s[14] = "4";
    s[19] = hexDigits.substr((s[19] & 0x3) | 0x8, 1);
    s[8] = s[13] = s[18] = s[23] = "-";

    var uuid = s.join("");
    return uuid;
}

function expandLots(t) {
    $("input#showLotsInfoHidden").attr("value", t.is(":checked"));
    if (t.is(":checked")) {
        $("td.lotsInfo").each(function () {
            $(this).find("p").addClass("expandP");
            $(this).find("div").first().slideDown(300);
        });
    } else {
        $("td.lotsInfo").each(function () {
            $(this).find("p").removeClass("expandP");
            $(this).find("div").first().slideUp(300);
        });
    }
}

function newExternalPopupLinkOnclick(elem, event, popupCss, ajaxUseCache) {
    var url = $(elem).attr("href");
    var shablonPattern = $(elem).attr("shablon-pattern");
    console.log(shablonPattern);
    if (event.preventDefault) {// non-IE browsers
        event.preventDefault();
    } else { // IE Only
        event.returnValue = false;
    }
    if (!url) {
        return;
    }
    var widthParam = "";
    var classList = $(elem).attr('class').split(/\s+/);
    $.each(classList, function (index, item) {
        if (item == 'newExternalPopUpLink') {
            return;
        }
        if (item.substring(0, 7) == "pWidth_") {
            widthParam = item.substring(7);
        }
    });

    var styleParam = "";
    if (widthParam != "") {
        styleParam = styleParam + " width:" + widthParam + "px;";
    }
    openPopupDialog(url, ajaxUseCache, popupCss, styleParam, {
        preprocessHtml: function (html) {
            switch (shablonPattern) {
                case 'controls':
                    renderControlsShablonPattern(html,true);
                    break;
                case 'controlsModal':
                    renderControlsShablonPattern(html,false);
                    break;
                case '223FZ':
                    render223FZShablonPattern(html,true);
                    break;
                case '223FZModal':
                    render223FZShablonPattern(html,false);
                    break;
                default:
                    return;
            }

        }
    });
}

function render223FZShablonPattern(html,oldModal) {
    if ($(html).find('.popUpWrapper').length > 0) {
        var tmplHead, tmplBody, tmplFooter;
        var modal = $('<div/>', {class: "modal signsListPopup", id: "modal-edsContainer", role: "dialog"});
        var modalContent = $('<div/>', {class: "modal-content modal-content-edsContainer edsContainer"});
        var tmplName = oldModal ? "/js/templates/signTemplate.htm" : "/js/templates/signTemplateModal.htm";
        $.get({
            url: "/epz/static" + tmplName,
            cache: true
        }).then(
            function (templates) {
                tmplHead = $(templates).filter('#tpl-sign-head').html();
                tmplBody = $(templates).filter('#tpl-sign-body').html();
                tmplFooter = $(templates).filter('#tpl-sign-footer').html();
                const tmplDataHead = {};
                //contextPath
                tmplDataHead.contextPath = contextPath;
                modalContent.append(Mustache.render(tmplHead, tmplDataHead));
                const tmplData = {};
                // parse html
                const tr = $(html).find('.popup-signBody > div > table > tbody > tr');
                // set status
                const status = $(html).find("div.elSignName span");
                // set fio
                const fio = $(html).find("div.elSignName h3");
                // set org
                const org = $(html).find("div.elSignDescr tr:nth-child(1) > td:nth-child(2)");
                // set  date
                const date = $(html).find("div.elSignDescr tr:nth-child(2) > td:nth-child(2)");
                // set cert
                const cert = $(html).find("div.elSignDescr tr:nth-child(3) > td:nth-child(2)");
                // set issued by
                const issued = $(html).find("div.elSignDescr tr:nth-child(4) > td:nth-child(2)");
                // set period
                const period = $(html).find("div.elSignDescr tr:nth-child(5) > td:nth-child(2)");
                // set sign body
                const signBody = $(html).find("div.elSignBox");

                let issued_text;
                let period_text;
                let from;
                let to;
                for (let i = 0; i < fio.length; i++) {
                        tmplData.fio = $(fio[i]).text().trim();
                        tmplData.status = $(status[i]).attr('class').search(/greenCheck/) >= 0
                            ? 'check signsListPopup-h3'
                            : 'uncheck signsListPopup-h3';
                        tmplData.org = $(org[i]).text().trim();
                        tmplData.date = $(date[i]).text().trim();
                        tmplData.cert = $(cert[i]).text().trim();
                        issued_text = $(issued[i]).text().trim();
                        tmplData.issued = getIssuedText(issued_text);
                        period_text = $(period[i]).text().trim()
                        from = period_text.slice(period_text.indexOf('с') + 2, period_text.indexOf('по') - 1);
                        to = period_text.slice(period_text.indexOf('по') + 3, period_text.length);
                        tmplData.period = from + ' - ' + to;
                        tmplData.signBody = $(signBody[i]).text().trim();

                        modalContent.append(Mustache.render(tmplBody, tmplData));
                    }
                    modalContent.append(Mustache.render(tmplFooter));
                    modal.append(modalContent);
                    $('div.popUp').html(modal);
                    /**
                     обработчик для отображения ЭЦП в модальном окне
                     */
                    if (oldModal) {
                        $('div.popUp').find(selectorToSpanClassExpandTr).closest("td").onSwitch(function () {
                                $(this).closest("table").find(selectorToTrClassToggleTr).slideDown(300, resizePopupDialog);
                                $(this).find("span").addClass("collapseTr");
                                $(this).next("td").find("span:eq(0)").hide();
                                $(this).next("td").find("span:eq(1)").fadeIn(300);
                                resizePopupDialog();
                            },
                            function () {
                                $(this).closest("tr").nextAll(selectorToTrClassToggleTr).eq(0).slideUp(300, resizePopupDialog);
                                $(this).find("span").removeClass("collapseTr");
                                $(this).next("td").find("span:eq(1)").hide();
                                $(this).next("td").find("span:eq(0)").fadeIn(300);
                            });
                        resizePopupDialog();
                        customSelect();
                    } else {
                        setFunctionalSignsModal();
                    }
            });
    }
}

function getIssuedText(str) {
    if (str === null || typeof str === "undefined" || str.length < 3) {
        return str;
    }
    if (str[0] === '"' && str[str.length - 1] === '"') {
        return str.substring(1, str.length - 1);
    }

    return str;
}

function renderControlsShablonPattern(html,oldModal) {
    if ($(html).length > 0) {
        var tmplHead, tmplBody;
        var modal = $('<div/>', {class: "modal signsListPopup", id:"modal-edsContainer", role:"dialog"});
        var modalContent = $('<div/>', {class: "modal-content modal-content-edsContainer edsContainer"});
        var tmplName = oldModal ? "/js/templates/signTemplate.htm" : "/js/templates/signTemplateModal.htm";
        $.get({
            url: "/epz/static" + tmplName,
            cache: true
        }).then(
            function (templates) {
                tmplHead = $(templates).filter('#tpl-sign-head').html();
                tmplBody = $(templates).filter('#tpl-sign-body').html();
                var tmplDataHead = {};
                //contextPath
                tmplDataHead.contextPath = contextPath;
                modalContent.append(Mustache.render(tmplHead, tmplDataHead));
                // parse html
                var tr = $(html).find('.popup-signBody > div > table > tbody > tr');
                $.each(tr, function (i, v) {
                    var tmplData = {};
                    var trr = $(this).find("table > tbody > tr");
                    $.each(trr, function (i, v) {
                        // set fio & status
                        if (i == 0) {
                            tmplData.status = $(v).find("td > img").attr('src').search(/_ok_/) >= 0 ? 'check' : 'uncheck';
                            tmplData.fio = $(v).find("td > span").text().trim();
                        }
                        // set org & date
                        if (i == 1) {
                            tmplData.date = $(v).find("td table > tbody > tr:nth-child(2) > td:nth-child(2) > span").text().trim();
                            tmplData.org = $(v).find("td table > tbody > tr:nth-child(1) > td:nth-child(2) > span").text().trim();
                        }
                        // set sign body
                        if (i == 7) {
                            tmplData.signBody = $(v).find("div").text().trim();
                        }
                    });
                    if (Object.keys(tmplData).length) {
                        modalContent.append(Mustache.render(tmplBody, tmplData));
                        modal.append(modalContent);
                    }
                });
                $('div.popUp').html(modal);
                /**
                 обработчик для отображения ЭЦП в модальном окне
                 */
                if (oldModal) {
                    $('div.popUp').find(selectorToSpanClassExpandTr).closest("td").onSwitch(function () {
                            $(this).closest("table").find(selectorToTrClassToggleTr).slideDown(300, resizePopupDialog);
                            $(this).find("span").addClass("collapseTr");
                            $(this).next("td").find("span:eq(0)").hide();
                            $(this).next("td").find("span:eq(1)").fadeIn(300);
                            resizePopupDialog();
                        },
                        function () {
                            $(this).closest("tr").nextAll(selectorToTrClassToggleTr).eq(0).slideUp(300, resizePopupDialog);
                            $(this).find("span").removeClass("collapseTr");
                            $(this).next("td").find("span:eq(1)").hide();
                            $(this).next("td").find("span:eq(0)").fadeIn(300);
                        });
                    resizePopupDialog();
                    customSelect();
                } else {
                    setFunctionalSignsModal();
                }
            });

    }
}

function setFunctionalSignsModal() {
    $('.btn-close').click(function(){
        $('.popupModalOverlay').remove();
    });
    $(".popupModalOverlay").removeClass("popUp");

    $(document).mouseup(function (e) {
        var container = $(".edsContainer").closest('.popupModalOverlay');
        if (!$(container).is(e.target) && container.has(e.target).length === 0){
            $(container).remove();
        }
        e.stopImmediatePropagation();
    });

    $('.expandSign').click(function (e) {
        e.preventDefault();
        var sign = $(this).closest('.row').find('.toggleSign');
        if (sign.hasClass('displayNone')) {
            sign.toggleClass('displayNone displayBlock');
            $(this).text("Скрыть электронную подпись");
        } else {
            sign.toggleClass('displayBlock displayNone');
            $(this).text("Показать электронную подпись");
        }
    })
}

//overlay+popUp
// You can add width param for popUp class
// Example:
// <a class="linkPopUp pWidth_740" href="${contextPath}/quiz/participation.html?timestamp=${date.time}">
function popupLinkOnclick(elem, event, popupCss, ajaxUseCache , callback) {
    var linkA = $(elem).attr("href");
    if (event.preventDefault) {// non-IE browsers
        event.preventDefault();
    } else { // IE Only
        event.returnValue = false;
    }
    if (!linkA) {
        return;
    }
    var widthParam = "";
    var classList = $(elem).attr('class').split(/\s+/);
    $.each(classList, function (index, item) {
        if (item == 'linkPopUp') {
            return;
        }
        if (item.substring(0, 7) == "pWidth_") {
            widthParam = item.substring(7);
        }
    });

    var styleParam = "";
    if (widthParam != "") {
        styleParam = styleParam + " width:" + widthParam + "px;";
    }
    if ($(elem).attr('ajaxUseCache') == 'false') {
        openPopupDialog(linkA, false, popupCss, styleParam);
        return;
    }
    if(typeof callback === 'function') {
        openPopupDialog(linkA, ajaxUseCache, popupCss, styleParam, callback);
    }else{
        openPopupDialog(linkA, ajaxUseCache, popupCss, styleParam);
    }
}

function openPopupDialog(url, ajaxUseCache, popupCss, styleParam, callbacks) {
    if (styleParam && styleParam != "") {
        styleParam = " style='" + styleParam + "'";
    }
    if (ajaxUseCache == undefined) {
        ajaxUseCache = true;
    }

    callbacks = callbacks || {};

    $("div.popupOverlay, div.popUp").remove();
    $("div.popupModalOverlay").remove();
    $("body").append("<div class='popupModalOverlay popUp'>");
    // $("div.popUp").append("<div class='popUp modal'" + styleParam + ">");orga
    // $("body").addClass("openmodal");

    $.ajax({
        url: url,
        type: 'GET',
        dataType: 'html',
        success: function (html) {
            if (typeof callbacks.preprocessHtml === 'function') {
                html = callbacks.preprocessHtml(html);
            }

            // $('div.popUp').html(html);
            $('div.popupModalOverlay').html(html);

            if (typeof callbacks.afterShown === 'function') {
                // callbacks.afterShown($('div.popUp'));
                callbacks.afterShown($('div.popupModalOverlay'));
            }

            if (popupCss) {
                // $("div.popUp").addClass(popupCss);
                $("div.popupModalOverlay").addClass(popupCss);
            }
            resizePopupDialog();
            customSelect();
            stopNavigation();
        },
        error: function (XMLHttpRequest, textStatus, errorThrown) {
            if (XMLHttpRequest.status != 901) {
                alert("Произошла ошибка");
            }

        }
    });
    if (typeof callbacks === 'function'){
        callbacks();
    }
}

function resizePopupDialog() {
    var windowHeight = $(window).height();
    // var containerHeight = $("div.outerWrapper").innerHeight();
    // var popUpHeight = $("div.popUpWrapper").innerHeight();
    // var popUpWidth = $("div.popUpWrapper").width();
    var containerHeight = $("div.modal-content").innerHeight();
    var popUpHeight = $("div.modal-content").innerHeight();
    var popUpWidth = $("div.modal-content").width();
    var popUpScroll = $(document).scrollTop();
    // var $popUpDiv = $("div.popUp:not(.notResizable)");
    var $popUpDiv = $("div.popupModalOverlay:not(.notResizable)");
    var poorVisionHeight = $('.poorVision .poorVisionSettingsBlockDiv2').height();
    var top = (windowHeight - containerHeight) / 2 - 30;
    $("div.modal-content").css('top', top < 0 ? 0 : top);
    /*
        if (($.cookie('usePoorVisionOption') == 'true') && popUpHeight > windowHeight - (poorVisionHeight * 2)) {
            if (containerHeight != null && poorVisionHeight != null && popUpHeight > windowHeight - poorVisionHeight) {
                $popUpDiv.css("height", containerHeight < windowHeight - poorVisionHeight ? (containerHeight - poorVisionHeight) + "px" : (windowHeight - poorVisionHeight) + "px");
            }
            $popUpDiv.css("margin-top", popUpScroll + "px");
            $popUpDiv.css("top", poorVisionHeight + "px");
        } else if (popUpHeight > windowHeight) {
            $popUpDiv.css("height", containerHeight < windowHeight ? containerHeight + "px" : windowHeight + "px");
            $popUpDiv.css("margin-top", popUpScroll + "px");
            $popUpDiv.css("top", "0px");
        } else {
            $popUpDiv.css("height", popUpHeight + "px");
            $popUpDiv.css("margin-top", popUpScroll - popUpHeight / 2 + "px");
            $popUpDiv.css("top", "");
        }
        $popUpDiv.css("margin-left", -popUpWidth / 2 + "px");
        */
}

function prepareVoteDialogLinks(selector) {
    preparePopupDialogLinks(selector, false, 'votePopupDialog');
}

function prepareNewExternalPopupDialogLinks(selector, ajaxUseCache, popupCss) {
    $(selector).on("click", function (event) {
        newExternalPopupLinkOnclick($(this), event, popupCss, ajaxUseCache);
    });
}

function preparePopupDialogLinks(selector, ajaxUseCache, popupCss) {
    $(document).on('click', selector, function (event) {
        popupLinkOnclick($(this), event, popupCss, ajaxUseCache);
    });
}

function prepareExternalSignLinks() {
    $('.externalSignLink').each(function () {
        $(this).click(function (e) {
            e.preventDefault();
            var source = $(this).data('source');
            var link = $(this).data('link');
            var entityId = $(this).data('entityId');
            externalSignPopupDialog(source, link, entityId);
        });
    });
}

function set_collapsed_state() {
    $("div.collapceBox").each(function () {
        if (this.id && $.cookie(this.id) && $.cookie(this.id) !== "null") {
            $(this).click();
        }
    });
}

//Произвести некоторые действия после события mouseleave только в том случае,
//если пользователь не навел курсор обратно в течение заданного времени mouseLeaveTimeout
function doAfterMouseLeaveCompletely(jqElement, mouseLeaveTimeout, callback) {
    var timeoutId = null;

    jqElement.mouseleave(function () {
        timeoutId = setTimeout(function () {
            callback();
        }, mouseLeaveTimeout);
    });

    jqElement.mouseover(function () {
        if (timeoutId) {
            clearTimeout(timeoutId);
            timeoutId = null;
        }
    });
}

function insertIntoIKZFields(t, data, array) {
    var pasteArray = [];
    var flag = false;
    var id = t.attr('name');
    if (data.length < 36) {
        array.forEach(function (value) {
            if (value === id) {
                flag = true;
            }
            if (flag) {
                pasteArray.push(value);
            }
        });
    } else {
        pasteArray = array;
    }
    var start = 0;
    var end = 0;
    pasteArray.forEach(function (value) {
        var input = $("[name='" + value + "']");
        var maxlength = parseInt(input.attr('maxlength'));
        input.val(data.substring(start, maxlength + end));
        start += maxlength;
        end += maxlength;
    });
}

$(document).ready(function () {
    var parent = $("[parent].secondTab").attr("parent");
    if (parent !== undefined) {
        $(("[tab='" + parent + "']")).addClass("parentTab");
    }
});
// инизилизация клика на вкладках в карточках
$(document).on("click", ".contentTabsWrapper td[tab]", function () {
    var url = $(this).attr('url');
    if (url.search(/^#/) == 0) {
        $("td[tab]").removeClass("currentTab");
        $(".tabs-container > ").hide();
        $(this).addClass("currentTab");
        $(url).show();
    } else {
        if (url && !$(this).hasClass("currentTab") && !$(this).hasClass("secondTab")) {
            window.location.href = $(this).attr('url');
        }
    }
});
var audioCaptcha = new Audio();
$(document).on("click", ".captcha-listen", function (e) {
    e.preventDefault();
    e.stopPropagation();
    audioCaptcha.pause();
    audioCaptcha.volume = 1;
    var url = $(this).find("a").attr("href");
    var http = window.location.href.split("/");
    if (http[0] + "//" + http[2] + url !== audioCaptcha.src) {
        audioCaptcha.src = url + "?_=" + new Date().getTime();
        audioCaptcha.load();
    }
    audioCaptcha.play();
});
$(document).on("click", ".captcha-repeat", function (e) {
    e.preventDefault();
    e.stopPropagation();
    var url = $(this).find("a").attr("href");
    var selectorImage = $(this).find("a").data("image");
    var selectorInput = $(this).find("a").data("input");
    if (url !== "#" && selectorImage.length > 0) {
        reloadCaptchaExt(url, selectorImage, selectorInput);
    }
});

$(document).on("click", ".poor-version-close", function (e) {
    e.stopPropagation();
    $("div.personalCabinetPopUp").hide();
});

function changeConstructModalUrl(id) {
    try {
        var link = $("#showConstructModal").attr('href');
        var urlArray = link.split("?");
        var paramsArray = urlArray[1].split("&");
        paramsArray.forEach(function (val, i) {
            console.log(val);
            if (val.indexOf("placeOfSearchListSelected") != -1) {
                var list = "";
                $("#" + id + " input:checked").each(function () {
                    switch ($(this).attr("name")) {
                        case "fz44":
                            list += "FZ_44,";
                            break;
                        case "fz94":
                            list += "FZ_94,";
                            break;
                        case "fz223":
                            list += "FZ_223,";
                            break;
                    }
                });
                list = list.substr(0, list.length - 1);
                paramsArray[i] = "placeOfSearchListSelected=" + list;
            }
        });
        link = urlArray[0] + "?" + paramsArray.join("&");
        $("#showConstructModal").attr('href', link);
    } catch (exception) {
        console.log(exception);
    }
}

function reloadCaptchaExt(url, selectorImage, selectorInput) {
    $(selectorImage).attr("src", url + "?_=" + new Date().getTime());
    $(selectorInput).val('');
    $(selectorInput).blur();
    // reset audio captcha
    audioCaptcha.src = '';
    showPopUpHint();
}

$(document).on("click", ".handlerClickForClearCookie", function () {
    clickForClearCookie($(this).data());
});

function clickForClearCookie(data) {
    $.cookie(data.default, null, {path: contextPath, expires: -1});
    $.cookie(data.current, null, {path: contextPath, expires: -1});
    window.location.href = data.url;
}

function collapseNavMenu() {
    if ($.cookie('usePoorVisionOption') != 'true') {
        menuIsCollapsed = true;
        var navigationDiv = $("div.navigation");
        $("div.overlay").remove();
        $("ul.nextLevel").slideUp(200);
        //$(this).css("clip","rect(0px, 50px, auto, 0px)");
        navigationDiv.stop(true, true).animate({width: "50px"}, 300);
        navigationDiv.find(".fixed").stop(true, true).animate({width: "50px"}, 300);
        navigationDiv.find(selectorClassTopLevel).stop(true, true).animate({width: "50px"}, 300);
        const selectorUlTopLevelClass = "ul" + selectorClassTopLevel;
        $(selectorUlTopLevelClass).find("li").removeClass("expandStatus");
        $('.nextLevel li.current').parents('ul').prev(selectorUlTopLevelClass).find('li').addClass('current');
    }
}

function expandNavMenu(element) {
    if ($.cookie('usePoorVisionOption') != 'true') {
        menuIsCollapsed = false;
        $(element).animate({width: "310px"}, 300);
        $(element).find(".fixed").animate({width: "310px"}, 300);
        $(element).find(selectorClassTopLevel).animate({width: "310px"}, 300);
        $('li.current').parents('ul').prev('ul' + selectorClassTopLevel).find('li').removeClass('current');
        if ($('div.overlay').length == 0) {
            $("body").append("<div class='overlay menuZIndex'>");
            $('li.current').parents('ul').stop(true, true).slideDown(300);
        }
    }
}

function stopNavigation() {
    mainNavigationTrack = clearTrack(mainNavigationTrack);
    innerNavigationTrack = clearTrack(innerNavigationTrack);
    lastNavigationTrack = clearTrack(lastNavigationTrack);
}

function clearTrack(track) {
    if (track != null && !track.isEmpty()) {
        track.getCurrentElement().loseFocus();
        return null;
    }
    return track;
}

/**
 * отображение детальной информации на вкладках контрольных реестрах
 *
 * Два варианта селетора что бы работало как с ИД так и с классом
 *
 * @param t
 */
function showDetailDocumentInfo(t) {
    var id = "#detailDocumentInfo-" + $(t).data('id');
    if ($(id).length == 0) {
        id = ".detailDocumentInfo-" + $(t).data('id');
    }
    if ($(id).hasClass("displayNone")) {
        $(id).removeClass("displayNone");
        $(t).text("Скрыть детальную информацию")
    } else {
        $(id).addClass("displayNone");
        $(t).text("Показать детальную информацию");
    }
}

/**
 * отображение недействующих редакций докуметов в контрольных реестрах
 * @param t
 */
function showInactiveDocumentEdition(t) {
    if ($(t).hasClass("showInactiveDocument")) {
        $(t).text("Показать недействующие редакции документов");
        $(t).removeClass("showInactiveDocument");
        $(".inactiveDocumentInfo").addClass("displayNone");
        $(".noDocumentInformation").removeClass("displayNone");
        $(".inactiveDocumentInfoItem").addClass("displayNone");
        $(".notActualAddInfoLink").text("Показать детальную информацию");

    } else {
        $(t).addClass("showInactiveDocument");
        $(t).text("Скрыть недействующие редакции документов");
        $(".inactiveDocumentInfo").removeClass("displayNone");
        $(".noDocumentInformation").addClass("displayNone");
    }
}

/**
 * Копирование в буффер
 * @param text
 * @returns {boolean}
 */
function copyToClipboard(value) {
    if (window.clipboardData && window.clipboardData.setData) {
        // IE specific code path to prevent textarea being shown while dialog is visible.
        return clipboardData.setData("Text", value);

    } else if (document.queryCommandSupported && document.queryCommandSupported("copy")) {
        var textarea = document.createElement("textarea");
        textarea.textContent = value;
        textarea.style.position = "fixed";  // Prevent scrolling to bottom of page in MS Edge.
        document.body.appendChild(textarea);
        textarea.select();
        try {
            return document.execCommand("copy");  // Security exception may be thrown by some browsers.
        } catch (ex) {
            console.warn("Copy to clipboard failed.", ex);
            return false;
        } finally {
            document.body.removeChild(textarea);
        }
    }
}

function getURLParam(name) {
    var result = new RegExp('[\?&]' + name + '=([^&#]*)').exec(window.location.href);
    if (result == null) {
        return null;
    } else {
        return decodeURI(result[1]) || 0;
    }
}

function changeSubmitDate(options) {
    var dateTo = $(options).val();
    if (dateTo) {
        if (dateTo.split(" - ").length > 0) $(options).val(dateTo.split(" - ")[0]);
    }
}

function setURLParam(param, newVal, search) {
    var regex = new RegExp("([?;&])" + param + "[^&;]*[;&]?");
    var query = search.replace(regex, "$1").replace(/&$/, '');
    return (query.length > 2 ? query + "&" : "?") + (newVal ? param + "=" + newVal : '');
}

function showDetails(selector, show) {
    var $elements = $("div." + selector);
    var $button = $("a." + selector);
    if (show) {
        $($elements).show();
        $($button).text("Скрыть");
    } else if (!show) {
        $($elements).hide();
        $($button).text("Показать все (" + $($elements).parent().children('div:not(div[style="display: none;"]:not([class*="details"]), div.d-none, div.filter-title--small, div.expansion-block, div.not-use-to-switch-visibility)').length + ")");
    }
}

//Раскрытие выпадающих списков для PV
$(document).ready(function () {
    $('.pseudoSelect').click(function () {
        var activeElements = $('.pseudoSelectOpenList');
        if ($(activeElements).length >= 1 && !$(activeElements).is($(this))) {
            $(activeElements).removeClass('pseudoSelectOpenList');
            $(activeElements.parent().find('.tuningQuickSearchBox')).css('display', 'none');
        }

        $(this).toggleClass('pseudoSelectOpenList');
    });

    $('.pseudoSelect *:not(input)').click(function () {
        $(this).toggleClass('pseudoSelectOpenList');
    });
    $(document).on('click', '.tuningQuickSearchBlock > .pseudoInput', function () {
        $(this).toggleClass('pseudoInputActive');
    });
    $(document).on('click', '.tuningQuickSearchTitle', function () {
        $(this).parents('.pseudoInput').toggleClass('pseudoInputActive');
    });
    $('.help').on('click', function () {
        return false;
    });
});

var ASG = function (tagNode, tagName, count) {
    ASG.prototype.tagNode = tagNode;
    ASG.prototype.tagName = tagName;
    ASG.prototype.count = count;
};

ASG.prototype.init = function () {
    initDetails(this.tagNode, this.tagName, this.count);
};

ASG.prototype.observer = function (tagNode, tagName, count) {
    function initDetailsThis(){
        initDetails(tagNode, tagName, count);
    }
    return new MutationObserver(function
        (mutation) {
        if (mutation[0].attributeName === 'style') {
            initDetailsThis();
        }
    });
};

function initDetails(tagNode, tagName, count) {
    var status = setFlagDetails(document.querySelector(tagNode), tagName, count);
    if (status === true) {
        showDetails("details_" + tagName, false);
    }
}

var observerCommon = new MutationObserver(function
    (mutation) {
    if (mutation[0].type === 'childList') {
        var tagName = "listOrg";
        var tagNode = "div.listOrg";
        var status = setCommonFlagDetails(document.querySelector(tagNode), tagName);
        if (status === true) {
            showCommonDetails(tagNode, "details_" + tagName, false);
        }
    }
});

function showCommonDetails(container, selector, show) {
    $(container).find('a').remove();
    $(container).append($('<a>', {
        class: 'cat-text cat-text_small btnBtn ' + selector
    }));
    var data = {
        container: container,
        selector: selector,
        show: !show
    };
    $(container).find('a').on('click',function () {
        showCommonDetails(data.container, data.selector, data.show);
    });
    if (show) {
        $("li." + selector).show();
        $("a." + selector).text("Скрыть")
    } else if (!show) {
        $("li." + selector).hide();
        $("a." + selector).text("Показать все")
    }
}

function setCommonFlagDetails(container, selector, count) {
    var size = typeof count !== 'undefined' ? count : 3;
    $(container).find('li').removeClass("details_" + selector);
    $(container).find('li').show();
    $("a.details_" + selector).text("");
    var items = $(container).find("li:visible");
    $(items).each(function (i, elem) {
        if (i > size) {
            $(elem).addClass("details_" + selector);
        }
    });
    return $(container).find("li").hasClass("details_" + selector);
}

var observerDetailsPlacingChildWaysTag = new MutationObserver(function
    (mutation) {
    if (mutation[0].type === 'childList') {
        var tagName = "PlacingChildWaysTag";
        var tagNode = "#childrenWayList";
        var status = setFlagDetails(document.querySelector(tagNode), tagName);
        if (status === true) {
            showDetails("details_" + tagName, false);
        }
    }
});


var configObserverAll = {
    childList: true,
    attributes: true,
    subtree: true
};

function setFlagDetails(container, selector, count) {
    var block = $(container).closest('.block');
    var visible = false;
    var size = typeof count !== 'undefined' ? count : 3;

    $(container).find("div").removeClass("details_" + selector);
    $("a.details_" + selector).text("");

    visible = $(block).find('.expand-module-expanded').length ? false : toggleExpandModule(visible, block);
    var items = $(container).hasClass("many-checkbox") ? $(container).find("div.search-group:visible") : $(container).find("div.search_tag:visible");
    visible = toggleExpandModule(visible, block);

    $(items).not(".not-use-to-switch-visibility").each(function (i, elem) {
        if (i > size) {
            $(elem).addClass("details_" + selector);
        }
    });

    return $(container).find("div").hasClass("details_" + selector);
}

function generateGuid() {
    return Math.random().toString(36).substring(15) + (new Date()).getTime().toString(36);
}

function clearAllSearchForm() {
    $('#customer').val("");
    $('#subjectControl').val("");
    $('#сontrolAgency').val("");
    $('#searchString').val("");
    $('.search .searchField .clear__btn').hide();
    var components = $('#searchOptionsEditContainer, ul.saveExtendedSearch,#quickSearchForm_header .quick-search-param');
    components.find(".edit-block:not([id^='placeOfSearch'])").removeClass('edit-block');
    components.find('div.block div.search_tag, div.block div.search_tag div[id$=Tag]').trigger("setDefaultValues");
    $('.searchBlockAll').trigger("setDefaultValues");
    showHints();
    $("div.errorRed").remove();
    $(".customInvalid").removeClass("customInvalid");
    $(".noValidField").removeClass("noValidField");
    $("span.removeDrugEl.removeDrugElKtruRubricator").click();
}

function initHint(input) {
    $(input).focus(function () {
        if (this.value === this.title) {
            $(this).prop("value", "").removeClass("color-gray");
            $("ul.ui-widget-content").addClass('popupAutocomplete');
        }
    }).blur(function () {
        if (this.value === this.title || this.value === '') {
            $(this).prop("value", this.title).addClass("color-gray");
        }
    }).each(function () {
        if (this.value === this.title || this.value === '') {
            $(this).prop("value", this.title).addClass("color-gray");
        }
    });
}

function showMoreItem(elem, selector, textVariant) {
    var text = textVariant ?
        'Показать все (' + $(elem).attr('count') + ')' :
        'Выбрано ещё ' + $(elem).attr('count');

    if ($(elem).hasClass("show")) {
        $(elem).removeClass("show");
        $(elem).text('Скрыть');
        $(elem).parent().find('.moreItem' + selector).removeClass('displayNone');
        $(elem).parent().find('.moreItem' + selector).show();
    } else {
        $(elem).addClass("show");
        $(elem).text(text);
        $(elem).parent().find('.moreItem' + selector).addClass('displayNone');
        $(elem).parent().find('.moreItem' + selector).hide();
    }
}

//Склонение числительных
function declOfNum(number, titles) {
    cases = [2, 0, 1, 1, 1, 2];
    return titles[(number % 100 > 4 && number % 100 < 20) ? 2 : cases[(number % 10 < 5) ? number % 10 : 5]];
}

//Для творительного падежа отличается
function suffix(number, titles) {
    cases = [2, 0, 1, 1, 1, 1];
    return titles[(number % 100 > 4 && number % 100 < 20) ? 1 : cases[(number % 10 < 5) ? number % 10 : 5]];
}
// Работа с подсказками в модальных окнах подсказки
function setFunctionalInfo(tempText) {
    $('.struct-block__unit .title').click(function() {
        var related = $(this).data('related');
        var activeElem = $('.modal-content-info .active');
        if ($(this).get(0) == activeElem.find('.title').get(0)) {
            activeElem.removeClass('active');
            $('.struct-block-info').html('<div class="temp-text">'+tempText+'</div>');
        } else {
            activeElem.removeClass('active');
            $('.struct-block-info').html($(this).data('content'));
            $(this).parent().addClass('active');
            $('.struct-block__unit[data-related="'+related+'"]').addClass('active');
        }
    });
}

// Для удаления пустых блоков в новом дизайне от версии 9.0
function hideEmptyBlocks(callBack) {
    var visible = false;
    $(".block").removeClass('d-none');
    $('.block').each(function () {
        visible = toggleExpandModule(visible, $(this));
            if(typeof callBack === "function"){
                callBack();
            }

            if ($(this).find('.row *:visible').length === 0) {
                $(this).addClass('d-none');
            }
        visible = toggleExpandModule(visible, $(this));
    });

    if ($('.block:nth-child(2) .row').find('div:visible:first')[0] == $('#okpdIdsTag')[0]) {
        $('.block:nth-child(2) #okpdIdsTag').removeClass('group-separator_left');
    } else {
        $('.block:nth-child(2) #okpdIdsTag').addClass('group-separator_left');
    }
    $(".block").find(".edit-block").closest(".block").find(".expand-module:not('.expand-module-expanded')").click();
}

function toggleExpandModule(visible, $this) {
    if(visible){
        $($this).find('.row:first').addClass('d-none');
        $($this).find('.expand-module').removeClass('expand-module-expanded');
        visible = false;
    }else {
        if($($this).find('.row').siblings('.expand-module').length) {
            if($($this).find('.row').is(':hidden')) {
                $($this).find('.row:first').removeClass('d-none');
            }
            visible = true;
        }
    }
    return visible;
}

//Получаем имя реестра
function getregistryName() {
    var tempBitLink = window.location.pathname.split("/epz/");
    var registry = tempBitLink[1].substring(0, tempBitLink[1].indexOf('/'));
    if (registry === "main") {
        if (tempBitLink[1].indexOf("news") > 0 || tempBitLink[1].indexOf("document") > 0) {
            var tempRegistry = tempBitLink[1].split("/public/");
            registry = tempRegistry[1].substring(0, tempRegistry[1].indexOf('/'));
        }
    }
    return registry;
}

function _serializeForm(formSelector) {
    return $(formSelector).find('input').filter(function () {
        return !!this.value && !!this.name;
    }).serialize();
}

function totalItems(selector, total) {
    if(typeof get_name_records === "function")
        $(selector).text((
            function () {
                var val = total;
                if (val) {
                    val = val.concat(' ').concat(get_name_records(val));
                }
                return val;
            }
        )());
}
function selectedItemsCountModal(selector, count){
    if(count) {
        $(selector).text((
            function () {
                var val = count;
                if (val) {
                    val = 'Выбрано значений: '.concat(val);
                }
                return val;
            }
        )());
    }else {
        $(selector).text('');
    }
}

if (!Object.entries) {
    //Polyfill for IE
    Object.entries = function( obj ){
        var ownProps = Object.keys( obj ),
            i = ownProps.length,
            resArray = new Array(i);
        while (i--)
            resArray[i] = [ownProps[i], obj[ownProps[i]]];

        return resArray;
    };
}

function listenerMoreItem(container, tagId, children, listOpen) {

    var list = $(container).children(children ? children : "li");
    $(list).removeClass('moreItem'+tagId).show();
    $(list).each(function (i, item) {
        if(i>3){
            listOpen ? $(item).addClass('moreItem'+tagId) : $(item).addClass('moreItem'+tagId).hide();
        }
    });

    var moreItem = $(list).size() - 4;

    if(listOpen){
        moreItem > 0 ? $(container).find('.cursorPointer').attr('count', moreItem) : $(container).find('.cursorPointer').remove();
    }else {
        $(container).find('.cursorPointer').remove();
        if (moreItem > 0) {
            var button = $('<a>', {
                class: 'cursorPointer show',
                'count': moreItem,
                text: 'Выбрано ещё ' + moreItem,
                onclick: 'showMoreItem(this, "' + tagId + '")'
            });
            $(container).append(button);
        }
    }
}



function groupSeparatorLeftToggle(items, check) {
    if (check) {
        $(items).filter(':visible:odd').addClass('group-separator_left');
        $(items).filter(':visible:even').removeClass('group-separator_left');
    } else {
        $(items).filter(':visible:even').removeClass('group-separator_left');
        $(items).filter(':visible:odd').addClass('group-separator_left');
    }
}

function paginationSubmitForm(page, pageSize, tableId) {
    if (!tableId) {
        return;
    }

    var urlRefresh = $("#" + tableId + "UrlRefresh");

    if ($(urlRefresh).val() === undefined) {
        return;
    }

    var data = {};
    data.page = page;

    if (pageSize == null) {
        pageSize = $("[data-selected-value]").data("selected-value");
    }

    data.pageSize = pageSize;

    $.ajax({
        data: data,
        url: $(urlRefresh).val(),
        success: function (html) {
            $("." + tableId + "InnerHtml").html(html);

            if(pageSize !== undefined && page === 1) {
                var containerId = tableId + "Position";
                var renderPaginationFunction = window[containerId + "PaginationContainerRenderPagination"];

                if (typeof  renderPaginationFunction === "function") {
                    renderPaginationFunction.apply(null, [pageSize, containerId]);
                }
            }
        }
    });
}

$(document).on('click','.password-icon', function(){
    var $this = $(this).parent().find('input');
    if ($this.attr('type') == 'password'){
        $(this).addClass('view');
        $this.attr('type', 'text');
    } else {
        $(this).removeClass('view');
        $this.attr('type', 'password');
    }
    return false;
});

function highLight(rawText, searchWord) {
    if (!searchWord) {
        return rawText;
    }

    const reg = new RegExp('(^|>)([^<]*)(' + searchWord.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&') + ')', 'gi');

    return rawText.replace(reg, function (match, prefix, before, word) {
        return prefix + before + '<span class="highlightColor">' + word + '</span>';
    });
}

function saveReferrerUrl() {
    const referrerUrl = document.referrer;
    const locationUrl = document.location.href;
    sessionStorage.setItem(contextPath, localStorage.getItem(contextPath));

    if (referrerUrl.length > 0) {
        const cardKey = "cardUrl" + charHashCode(locationUrl);
        const isReferrerEis = (referrerUrl.indexOf("result") > 0|| referrerUrl.indexOf("search_eis") > 0) && referrerUrl.indexOf("search") > 0 ;

        if (isReferrerEis) {
            const searchKey = "searchUrl" + charHashCode(referrerUrl);
            sessionStorage.setItem(searchKey, referrerUrl);
            sessionStorage.setItem(cardKey, searchKey);
        } else {
            const previousCardKey = "cardUrl" + charHashCode(referrerUrl);
            const searchKey = sessionStorage.getItem(previousCardKey);
            if (searchKey !== null && searchKey.length > 0) {
                sessionStorage.setItem(cardKey, searchKey);
                sessionStorage.removeItem(previousCardKey);
            }
        }
    }
}

function goToCardUrl(homeUrl, isCustomUrl, valid) {
    const ref = document.referrer;
    if(!isCustomUrl) {
        if (ref.indexOf("epz") > 1 && valid) {
            window.location = document.referrer;
        } else if (homeUrl) {
            window.location = homeUrl;
        }
    }else{
        window.location = homeUrl;
    }
}

function goToSearchUrl(card, homeUrl, isKtru) {
    if (card) {
        let url = contextPath.indexOf("order") > 0 ? contextPath.replace("notice", "extendedsearch") : contextPath;
        let postUrl = sessionStorage.getItem(url);
        let cardKey = "cardUrl" + charHashCode(document.location.href);
        let searchKey = sessionStorage.getItem(cardKey);

        if (postUrl && !isGet(postUrl)) {
            if (postUrl && postUrl !== "null") {
                redirect(localStorage.getItem(url + "url"), 'post', postUrl.split('&'));
            }
        } else {
            let valid = storageValid(searchKey);
            if (valid) {
                doGet(cardKey, searchKey);
            }else {
                goToCardUrl(homeUrl, false, valid);
            }
        }
    } else {
        if(homeUrl){
            let name = isKtru ? getKtruName(homeUrl) : homeUrl;
            if(name !== null){
                let url = sessionStorage.getItem(name);
                if(name!== null && url !== null){
                    window.location = url;
                }else{
                    window.location = document.referrer;
                }

                sessionStorage.removeItem(name);
            }
        }else{
            window.location = document.referrer;
        }
    }
}

const getKtruName = function(name){
   if(name && name !== undefined){
      if(name.indexOf(':') > 0){
          name = name.split(':')[0];
          return name.replace('[^0-9.-]', ' ').replace(' ', '');
      }
   }

   return null;
}

const storageValid = function(searchKey){
    return searchKey && searchKey !== null && searchKey.length > 0;
}

function charHashCode(s){
    return s.split("").reduce(function(a,b){a=((a<<5)-a)+b.charCodeAt(0);return a&a},0);
}

function stringIsNotEmpty(val) {
    return typeof(val) !== 'undefined' && val.length > 0;
}

var isGet = function(urlLength){
    //Microsoft Internet Explorer has a maximum get/post length of 2,048 characters.
    return urlLength.length < 2048;
}

var redirect = function(url, method, params) {
    var form = document.createElement('form');
    form.method = method;
    form.action = url;
    for (var i in params)
    {
        var input = document.createElement('input');
        var param = params[i].split('=');
        input.type = 'hidden';
        input.name = param[0];
        input.value = param[1] != undefined ? decodeURIComponent(param[1]) : '';
        form.appendChild(input);
    }
    document.body.appendChild(form)
    form.submit();
}

const doGet = function(cardKey, searchKey){
    const searchUrl = sessionStorage.getItem(searchKey);
    if (searchUrl == null || searchUrl.length === 0) {
        return;
    }
    sessionStorage.removeItem(cardKey);
    for (let i = 0; i < sessionStorage.length; i++) {
        const key = sessionStorage.key(i);
        if (key !== undefined && key.indexOf("cardUrl") > 0) {
            const val = sessionStorage.getItem(key);
            if (val !== undefined && val.indexOf("searchKey") < 0) {
                sessionStorage.removeItem(searchKey);
            }
        }
    }
    window.location = searchUrl;
}


const mergeTwoProps = function(propsOne, propsTwo) {
    const result = {};
    for (let propsOneAttrName in propsOne) { // NOSONAR EPZ-54449 - "const in for-of loop iteration scope" is not supported in IE
        result[propsOneAttrName] = propsOne[propsOneAttrName];
    }
    for (let propsTwoAttrName in propsTwo) { // NOSONAR EPZ-54449 - "const in for-of loop iteration scope" is not supported in IE
        result[propsTwoAttrName] = propsTwo[propsTwoAttrName];
    }
    return result;
}
