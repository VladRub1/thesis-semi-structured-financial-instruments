navigationRules = {
    'extendedTuningQuickSearchBlock' : [
        {
            'order' : [
                '\'.tuningQuickSearchTitle\'',
                '\'.editingBtn\'',
                '\'.delBtn\''
            ],
            'default' : 0,
            'parentContainer' : 'li'
        }
    ],    
    'resultSearch' : [
        {
            'order' : [
                '\'div.registerBox td.lotsInfo p\'',
                '\'div.registerBox .reportBox li:first-child a\''
            ],
            'default' : 1
        },
        {
            'order' : [
                '$(".descriptTenderTd a")',
                '$(".nameOrganization a")',
                '$(".boxIcons a, .linksWrapper .printLink")',
                '$(".boxIcons a, .linksWrapper .controlLink")',
                '$(".boxIcons a")',
                '$(".statusWarn a")'
            ],
            'default' : 0
        }
    ],

    'searchFormArray' : [
        {
            'order' : [
                '\'div.searchField input#searchString, div.searchField input#quickSearchForm_searchString, div.searchField input#quickSearchForm_header_searchString\'',
                '$(\'div.searchBlockAll input#morphology\').add($(\'div.searchBlockAll input#morphology\').parent(".customCheckbox"))',
                '$(\'div.searchBlockAll input#strictEqual\').add($(\'div.searchBlockAll input#strictEqual\').parent(".customCheckbox"))'
            ],
            'default' : 0
        },
        {
            'order' : [
                '\'div.saveExtendedSearch span#clearAll\'',
                '\'div.saveExtendedSearch span.searchBtn\''
            ],
            'default' : 0
        },
        {
            'order' : [
                '\'li#setParametersLink a\'',
                '\'div.filterParametrs div.collapceBox\''
            ],
            'default' : 1
        }
    ],
    'sortBy' : [
        {
            'order' : [
                '\'div.sortBy li.sortItem:last-child\'',
                '\'div.sortBy > ul > li.pageSelect > span\''
            ],
            'default' : 0
        }
    ],
    'newsBlock' : [
        {
            'order' : [
                '\'div.newsBlock a.rssBox\'',
                '\'div.newsBlock span.btnBrd\''
            ],
            'default' : 0
        }
    ]

}