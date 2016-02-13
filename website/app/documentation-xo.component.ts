import {Component} from 'angular2/core';

import {HighlighterComponent} from './highlighter.component';
import {DocumentationSelectorComponent} from './documentation-selector.component';

@Component({
	selector: 'documentation-xo',
	directives: [
	  HighlighterComponent,
	  DocumentationSelectorComponent,
	],
	templateUrl: '../html/documentation-xo.html'
})
export class DocumentationXOComponent {
}

