$(document).ready(function () {
    $(document).on("click", ".general-chevron-handler", generalChevronHandler);
});

const GENERAL_CHEVRON_STATUS_DATA = "data-chevron-status";

function generalChevronHandler(event) {
    const targetJqr = $(event.target);
    const chevronJqr = targetJqr.closest(".general-chevron-handler");
    const chevronContentId = chevronJqr.attr("data-content-id");
    if (chevronContentId == null || chevronContentId === "") {
        return;
    }
    const chevronContentJqr = $("#" + chevronContentId);

    if (chevronJqr.attr(GENERAL_CHEVRON_STATUS_DATA) === "opened") {
        // Шеврон открыт
        _generalChevronClose(chevronJqr, chevronContentJqr);
    } else {
        // Шеврон закрыт
        if (chevronJqr.attr("data-has-content") === "true") {
            _generalChevronOpen(chevronJqr, chevronContentJqr);
        } else {
            const url = chevronJqr.attr("data-url");
            $.ajax({
                url: url,
                success: function (html) {
                    chevronContentJqr.html(html);
                    chevronJqr.attr("data-has-content", "true");
                    _generalChevronOpen(chevronJqr, chevronContentJqr);
                }
            });
        }
    }
}

function _generalChevronOpen(chevronJqr, chevronContentJqr) {
    chevronJqr.removeClass("rl-90");
    chevronContentJqr.slideDown();
    chevronJqr.attr(GENERAL_CHEVRON_STATUS_DATA, "opened");
}

function _generalChevronClose(chevronJqr, chevronContentJqr) {
    chevronJqr.addClass("rl-90");
    chevronContentJqr.slideUp();
    chevronJqr.attr(GENERAL_CHEVRON_STATUS_DATA, "closed");
}
