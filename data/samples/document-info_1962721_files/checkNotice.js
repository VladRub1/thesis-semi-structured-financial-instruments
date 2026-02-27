//Критическая уведомление
function checkNotice() {
    $.ajax({
        url: epzMainPublicUrl + 'news/check-notice.json',
        method: "GET",
        success: function (data) {
            if (data.isFailure) {
                console.log(data.errorMsg);
            } else {
                if (data.id != 0 && Cookies.get('critical_notice_crc') != data.crc && !$("#critical_notice").children().length) {
                    var tmplData = {
                        title: data.title,
                        preview: data.preview,
                        crc: data.crc,
                        id: data.id,
                        epzMainPublicUrl: epzMainPublicUrl,
                        contextPath: contextPath
                    };
                    var noticeTmpl;
                    $.get({
                        url:'/epz/static/js/templates/analyticsStatisticsTemplates.htm',
                        cache: true
                    }).then(
                        function (templates) {
                        noticeTmpl = $(templates).filter('#critical_notice_tmpl').html();
                        $('#critical_notice').append(Mustache.render(noticeTmpl, tmplData));
                        $(document).on('click','.button-close-alert', function () {
                            Cookies.set('critical_notice_crc', tmplData.crc);
                            $('#critical_notice').hide();
                        });
                    }
                    );
                }
            }
        }
    });
}