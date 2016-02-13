import {Component} from 'angular2/core';

import {HighlighterComponent} from './highlighter.component';
import {DocumentationSelectorComponent} from './documentation-selector.component';

@Component({
	selector: 'documentation-compile',
	directives: [
	  HighlighterComponent,
	  DocumentationSelectorComponent,
	],
	templateUrl: '../html/documentation-compile.html'
})
export class DocumentationCompileComponent {
}

