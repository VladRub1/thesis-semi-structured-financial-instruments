function showHints() {
    $(".hint").each(function () {
        var newValue = this.title;
        if ($(this).prop('id') == "quickSearchForm_header_searchString" || $(this).prop('id') == "extendedSearchForm1_searchString") {
            newValue = reduceTextByWidth($(this));
        }
        if (this.value === newValue || this.value === $(this).data("lastValue") || this.value == '') {
            $(this).prop("value", newValue).css("color", "#999999");
            $(this).data("lastValue", newValue);
        }
    });
}

function deleteHints() {
    $(".hint").each(function () {
        if (this.value === this.title || this.value === $(this).data("lastValue") || this.value == '') {
            $(this).val('');
        }
    });
}