const GENERAL_PAGINATION_SHOW_BY_HANDLER_JQRY_SELECT = '.general-pagination-show-by-handler';
const GENERAL_PAGINATION_BLOCK_JQRY_SELECT = '.general-pagination-block';
const GENERAL_PAGINATION_STATUS_DATA = 'data-status';
const GENERAL_PAGINATION_CONTENT_ID_DATA = 'data-pagination-content-id';

$(document).ready(function () {
    $(document).on("click", GENERAL_PAGINATION_SHOW_BY_HANDLER_JQRY_SELECT, generalPaginationShowByHandler);
    $(document).on("click", ".general-pagination-page-select-handler", generalPaginationPageSelectHandler);
    $(document).on("click", generalPaginationShowByMissedClickHandler);
});

function generalPaginationShowByHandler(event) {
    // Показывать по: ...
    const targetJqr = $(event.target);
    const paginationBlockJqr = targetJqr.closest(GENERAL_PAGINATION_BLOCK_JQRY_SELECT);
    const handlerElementJqr = $(GENERAL_PAGINATION_SHOW_BY_HANDLER_JQRY_SELECT, paginationBlockJqr);
    const opened = handlerElementJqr.attr(GENERAL_PAGINATION_STATUS_DATA) === "opened";
    if (opened) {
        _generalPaginationCloseShowBy(handlerElementJqr);
    } else {
        _generalPaginationOpenShowBy(handlerElementJqr);
    }
    const recordPerPageJqr = $(".select-record-per-page--number", paginationBlockJqr);
    const selectedElementJqr = targetJqr.closest(".gp-select-vars__item");
    if (selectedElementJqr.length > 0) {
        const newPageSize = selectedElementJqr.text();
        const oldPageSize = recordPerPageJqr.attr("data-selected-value");
        if (newPageSize !== oldPageSize) {
            const itemIndex = paginationBlockJqr.attr("data-item-index");
            generalPaginationLoad(paginationBlockJqr, itemIndex, newPageSize);
        }
    }
}

function generalPaginationShowByMissedClickHandler(event) {
    // Клик по любой области, кроме списка "Показывать по" - закрыть список "Показывать по"
    const targetJqr = $(event.target);
    $(GENERAL_PAGINATION_SHOW_BY_HANDLER_JQRY_SELECT).filter("["+GENERAL_PAGINATION_STATUS_DATA+" = 'opened']").each(function (index, element) {
        const elementId = $(element).closest(GENERAL_PAGINATION_BLOCK_JQRY_SELECT).attr(GENERAL_PAGINATION_CONTENT_ID_DATA);
        const clickId = targetJqr.closest(GENERAL_PAGINATION_BLOCK_JQRY_SELECT).attr(GENERAL_PAGINATION_CONTENT_ID_DATA);
        if (elementId !== clickId || !targetJqr.hasClass("gp-on-click-no-close")) {
            _generalPaginationCloseShowBy($(element));
        }
    })
}

function generalPaginationPageSelectHandler(event) {
    // Выбор другой страницы
    const paginationBlockJqr = $(event.target).closest(GENERAL_PAGINATION_BLOCK_JQRY_SELECT);
    const elementJqr = $(event.target).closest(".general-pagination-page-select-handler");
    const recordPerPageJqr = $(".select-record-per-page--number", paginationBlockJqr);
    const pageSize = recordPerPageJqr.attr("data-selected-value");
    const newPageNum = elementJqr.attr("data-pagenumber");
    const newItemIndex = (newPageNum - 1) * pageSize;
    generalPaginationLoad(paginationBlockJqr, newItemIndex, pageSize);
}

function generalPaginationLoad(paginationBlockJqr, itemIndex, pageSize) {
    const paginationContentId = paginationBlockJqr.attr(GENERAL_PAGINATION_CONTENT_ID_DATA);
    if (paginationContentId == null || paginationContentId === "") {
        return;
    }
    const paginationContentJqr = $("#" + paginationContentId);
    const url = paginationBlockJqr.attr("data-base-url") + "itemIndex=" + itemIndex + "&pageSize=" + pageSize;
    $.ajax({
        url: url,
        success: function (html) {
            paginationContentJqr.replaceWith(html);
        }
    });
}

function _generalPaginationOpenShowBy(handlerElementJqr) {
    const selectVarsBlockJqr = $(".gp-select-vars", handlerElementJqr);
    selectVarsBlockJqr.addClass("select-vars_open").slideToggle();
    const iconJqr = $(".gp-select-icon", handlerElementJqr);
    iconJqr.addClass("arrow-select-vars_open");
    handlerElementJqr.attr(GENERAL_PAGINATION_STATUS_DATA, "opened");
}


function _generalPaginationCloseShowBy(handlerElementJqr) {
    const selectVarsBlockJqr = $(".gp-select-vars", handlerElementJqr);
    selectVarsBlockJqr.removeClass("select-vars_open").slideToggle();
    const iconJqr = $(".gp-select-icon", handlerElementJqr);
    iconJqr.removeClass("arrow-select-vars_open");
    handlerElementJqr.attr(GENERAL_PAGINATION_STATUS_DATA, "closed");
}
