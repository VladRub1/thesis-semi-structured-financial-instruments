/*
jQuery.msgBox plugin 
Copyright 2011, Halil İbrahim Kalyoncu
License: BSD
modified by Oliver Kopp, 2012.
 * added support for configurable image paths
 * a new msgBox can be shown within an existing msgBox
*/

/*
contact :

halil@ibrahimkalyoncu.com
koppdev@googlemail.com

*/


/*
 jQuery.msgBox plugi-in
 Copyright 2011, Halil İbrahim Kalyoncu
 */

var msgBoxImagePath = "/epz/static/img/icons/";

jQuery.msgBox = msg;
function msg (options) {
    var isShown = false;
    var typeOfValue = typeof options;
    var defaults = {
        content: (typeOfValue == "string" ? options : "Message"),
        title: "Сообщение",
        type: "alert",
        autoClose: false,
        timeOut: 0,
        extraClass: "",
        showButtons: true,
        buttons: [{ value: "Ok"}],
        inputs: [{ type: "text", name:"userName", header: "User Name" }, { type: "password",name:"password", header: "Password"}],
        success: function (result) { },
        beforeShow: function () { },
        afterShow: function () { },
        beforeClose: function () { },
        afterClose: function () { },
        opacity: 0.7
    };
    options = typeOfValue == "string" ? defaults : options;
    if (options.type != null) {
        switch (options.type) {
            case "alert":
                options.title = options.title == null ? "Предупреждение" : options.title;
                break;
            case "info":
                options.title = options.title == null ? "Сообщение" : options.title;
                break;
            case "error":
                options.title = options.title == null ? "Ошибка" : options.title;
                break;
            case "confirm":
                options.title = options.title == null ? "Подтверждение" : options.title;
                options.buttons = options.buttons == null ? [{ value: "Yes" }, { value: "No" }, { value: "Cancel"}] : options.buttons;
                break;
            case "prompt":
                options.title = options.title == null ? "Log In" : options.title;
                options.buttons = options.buttons == null ? [{ value: "Login" }, { value: "Cancel"}] : options.buttons;
                break;
            case "tinyUrl":
                break;
            case "info-message":
                break;
            case "reg":
                break;
            default:
                image = "icon_message_close.svg";
        }
    }
    options.timeOut = options.timeOut == null ? (options.content == null ? 500 : options.content.length * 70) : options.timeOut;
    options = $.extend(defaults, options);
    if (options.autoClose) {
        setTimeout(hide, options.timeOut);
    }
    var image = "";
    switch (options.type) {
        case "alert":
            image = "icon_message_close.svg";
            break;
        case "alert2":
            image = "msgbox_alert.png";
            break;
        case "info":
            image = "icon_message_ok.svg";
            break;
        case "error":
            image = "icon_message_close.svg";
            break;
        case "confirm":
            image = "icon_error_message.svg";
            break;
        case "tinyUrl":
            break;
        case "info-message":
            break;
        case "reg":
            image = "icon_message_ok_green.svg";
            break;
        default:
            image = "icon_message_close.svg";
    }

    var buttons = "";
    $(options.buttons).each(function (index, button) {
        buttons += "<input class=\"msgButton\" type=\"button\" name=\"" + button.value + "\" value=\"" + button.value + "\" />";
    });

    var inputs = "";
    $(options.inputs).each(function (index, input) {
        var type = input.type;
        if (type=="checkbox" || type =="radio") {
            inputs += "<div class=\"msgInput\">" +
                "<input type=\"" + input.type + "\" name=\"" + input.name + "\" "+(input.checked == null ? "" : "checked ='"+input.checked+"'")+" value=\"" + (typeof input.value == "undefined" ? "" : input.value) + "\" />" +
                "<text>"+input.header +"</text>"+
                "</div>";
        }
        else {
            inputs += "<div class=\"msgInput\">" +
                "<span class=\"msgInputHeader\">" + input.header + "<span>" +
                "<input type=\"" + input.type + "\" name=\"" + input.name + "\" value=\"" + (typeof input.value == "undefined" ? "" : input.value) + "\" />" +
                "</div>";
        }
    });

    var divBackGround = "<div class=\"msgBoxBackGround\"></div>";
    var divTitle;

    if (options.type === "info-message" || options.type === "reg") {
        divTitle = "<div class=\"msgBoxTitle\">" + options.title + "</div>";
    } else {
        divTitle = "<div class=\"msgBoxTitle\">" + options.title + "<span class='closePopUp' style='width: 26px !important'></span></div>";
    }

    var divButtons = "<div class=\"msgBoxButtons\">" + buttons + "</div>";
    var divInputs = "<div class=\"msgBoxInputs\">" + inputs + "</div>";
    var divContainer = "";

    if (options.type != "tinyUrl") {
        if (options.type === "info-message") {
            divContainer = "<div class=\"msgBoxContainer\"><div class=\"msgBoxContent\"><span>" + options.content + "</span></div></div>";
        } else if (options.type === "reg") {
            divContainer = "<div class=\"msgBoxContainer\"><div class=\"msgBoxContent\"><p><img src=\"" + msgBoxImagePath + image + "\"/>" + options.content + "</p></div></div>";
        } else {
            divContainer = "<div class=\"msgBoxContainer\"><div class=\"msgBoxImage\"><img src=\""+ msgBoxImagePath + image + "\"/></div><div class=\"msgBoxContent\">" + options.content + "</div></div>";
        }
    } else {
        divContainer = "" +
            "<div class=\"msgBoxContainer\">" +
                "<div class=\"browserIco\">Для RSS-подписки в браузере нажмите \"Продолжить\".</div>" +
                "<div class=\"eMailIco brdTop2 mb-0\" style=\"height: auto!important;\">Для RSS-подписки в почтовом клиенте скопируйте ссылку: <div class=\"text-break\">" + options.tinyUrl + "</div></div>" +
            "</div>";
    }

    if ($("div.msgBox").length == 0) {
        if (options.type == "prompt") {
            $("body").append(divBackGround + "<div class=\"msgBox " + options.extraClass + "\">" + divTitle + "<div>" + divContainer + (options.showButtons ? divButtons + "</div>" : "</div>") + "</div>");
            $("div.msgBoxImage").remove();
            $("div.msgBoxButtons").css({"text-align":"center","margin-top":"5px"});
            $("div.msgBoxContent").css({"width":"100%","height":"100%"});
            $("div.msgBoxContent").html(divInputs);
        }
        else {
            $("body").append(divBackGround + "<div class=\"msgBox " + options.extraClass + "\">" + divTitle + "<div>" + divContainer + (options.showButtons ? divButtons + "</div>" : "</div>") + "</div>");
            if (options.type === "info-message") {
                $("input.msgButton").css("margin-right", 0);
                $("div.msgBoxContent").css({"width":"inherit","height":"inherit"});
            }
            if (options.type === "reg") {
                $("input.msgButton").css("margin-right", 0);
                $("div.msgBoxContent").css({"width":"inherit","height":"inherit"});
                $("p span").css("color", "#99cc66");
            }
        }
    }
    else {
        if (options.type == "prompt") {
            $("div.msgBox").html(divTitle + "<div>" + divContainer + (options.showButtons ? divButtons + "</div>" : "</div>"));
            $("div.msgBoxImage").remove();
            $("div.msgBoxContent").css("width", "100%");
            $("div.msgBoxContent").html(divInputs);
        }
        else {
            $("div.msgBox").html(divTitle + "<div>" + divContainer + (options.showButtons ? divButtons + "</div>" : "</div>"));
        }
    }

    var width = $("div.msgBox").width();
    var height = $("div.msgBox").height();
    var windowHeight = $(window).height();
    var windowWidth = $(window).width();

    var top = windowHeight / 2 - height / 2;
    var left = windowWidth / 2 - width / 2;

    show();

    function show() {
        if (isShown) {
            return;
        }
        $("div.msgBox").css({ opacity: 0, top: top - 50, left: left });
        $("div.msgBoxBackGround").css({ opacity: options.opacity });
        options.beforeShow();
        $("div.msgBox").trigger("beforeShow");
        $("div.msgBoxBackGround").css({ "width": $(document).width(), "height": getDocHeight() });
        $("div.msgBox,div.msgBoxBackGround").fadeIn(0);
        $("div.msgBox").animate({ opacity: 1, "top": top, "left": left }, 200);
        setTimeout(options.afterShow, 200);
        isShown = true;
        $(window).bind("resize", function (e) {
            var width = $("div.msgBox").width();
            var height = $("div.msgBox").height();
            var windowHeight = $(window).height();
            var windowWidth = $(window).width();

            var top = windowHeight / 2 - height / 2;
            var left = windowWidth / 2 - width / 2;

            $("div.msgBox").css({ "top": top, "left": left });
        });
    }

    function hide() {
        if (!isShown) {
            return;
        }
        options.beforeClose();
        $("div.msgBox").animate({ opacity: 0, "top": top - 50, "left": left }, 200);
        $("div.msgBoxBackGround").fadeOut(300);
        setTimeout(function () { $("div.msgBox,div.msgBoxBackGround").remove(); }, 300);
        setTimeout(options.afterClose, 300);
        isShown = false;
    }

    function getDocHeight() {
        var D = document;
        return Math.max(
            Math.max(D.body.scrollHeight, D.documentElement.scrollHeight),
            Math.max(D.body.offsetHeight, D.documentElement.offsetHeight),
            Math.max(D.body.clientHeight, D.documentElement.clientHeight));
    }

    function getFocus() {
        $("div.msgBox").fadeOut(200).fadeIn(200);
    }

    $("input.msgButton").click(function (e) {
        e.preventDefault();
        var value = $(this).val();
        if (options.type != "prompt") {
            options.success(value);
        }
        else {
            var inputValues = [];
            $("div.msgInput input").each(function (index, domEle) {
                var name = $(this).attr("name");
                var value = $(this).val();
                var type = $(this).attr("type");
                if (type == "checkbox" || type == "radio") {
                    inputValues.push({ name: name, value: value,checked: $(this).attr("checked")});
                }
                else {
                    inputValues.push({ name: name, value: value });
                }
            });
            options.success(value,inputValues);
        }
        hide();
    });

    $("div.msgBoxBackGround").click(function (e) {
        if (!options.showButtons || options.autoClose) {
            hide();
        }
        else {
            getFocus();
        }
    });

    $(".msgBoxTitle .closePopUp").click(function (e) {
        e.preventDefault();
        e.stopPropagation();
        hide();
    });
};