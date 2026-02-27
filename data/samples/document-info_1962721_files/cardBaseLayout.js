$(document).ready(function() {
    $(".hint").not(".datepicker_ru").focus(function () {
        if (this.value === this.title || this.value === $(this).data("lastValue") || this.value == '') {
            $(this).prop("value", "").css("color", "#5b5b5b");
        }
    }).blur(function () {
        var newValue = this.title;
        if (this.value === this.title || this.value === $(this).data("lastValue") || this.value == '') {
            $(this).prop("value", newValue).css("color", "#999999");
            $(this).data("lastValue", newValue);
        }
    }).on("setActive", function () {
        $(this).css("color", "#5b5b5b");
    });
});
